// Package limiter is to control the links that go into the dispatcher
package limiter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/marshaler"
	"github.com/eko/gocache/lib/v4/store"
	goCacheStore "github.com/eko/gocache/store/go_cache/v4"
	redisStore "github.com/eko/gocache/store/redis/v4"
	goCache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"github.com/wyx2685/XrayR/api"
	"golang.org/x/time/rate"
)

type UserInfo struct {
	UID         int
	SpeedLimit  uint64
	DeviceLimit int
}

type InboundInfo struct {
	Tag            string
	NodeSpeedLimit uint64
	UserInfo       *sync.Map // Key: Email value: UserInfo
	BucketHub      *sync.Map // key: Email, value: *rate.Limiter
	UserOnlineIP   *sync.Map // Key: Email, value: {Key: IP, value: UID}
	// --- 新增字段 ---
	OldUserOnline  *sync.Map // Key: IP, value: UID
	LinkManagers   *sync.Map // Key: Email, value: *counter.XrayTrafficCounter
	// ---------------
	GlobalLimit    struct {
		config         *GlobalDeviceLimitConfig
		globalOnlineIP *marshaler.Marshaler
	}
	AliveList     map[int]int // Key: Uid, value: alive_ip
	OldUserOnline *sync.Map   // Key: Ip, value: Uid
}

// GetLinkManager 获取或初始化用户的连接计数器
func (d *InboundInfo) GetLinkManager(email string) *counter.XrayTrafficCounter {
	if v, ok := d.LinkManagers.Load(email); ok {
		return v.(*counter.XrayTrafficCounter)
	}
	newCounter := &counter.XrayTrafficCounter{V: new(atomic.Int64)}
	d.LinkManagers.Store(email, newCounter)
	return newCounter
}

func (d *InboundInfo) GetUserBucket(email string, ip string, protocol string) (Bucket *rate.Limiter, reject bool) {
	// 1. IP 规范化
	ip = strings.TrimPrefix(ip, "::ffff:")

	var deviceLimit int = 0
	var uid int = 0
	if v, ok := d.UserInfo.Load(email); ok {
		user := v.(UserInfo)
		deviceLimit = user.DeviceLimit
		uid = user.UID
	}

	var userIPMap *sync.Map
	if m, ok := d.UserOnlineIP.Load(email); ok {
		userIPMap = m.(*sync.Map)
	} else {
		userIPMap = new(sync.Map)
		d.UserOnlineIP.Store(email, userIPMap)
	}

	// 2. 检查当前周期是否已记录
	if _, ok := userIPMap.Load(ip); ok {
		return d.getSpeedBucket(email), false
	}

	// 3. 检查旧周期缓存 (v2node 核心逻辑：存量 IP 不计入新连接判定)
	if d.OldUserOnline != nil {
		if v, ok := d.OldUserOnline.Load(ip); ok {
			if v.(int) == uid {
				userIPMap.Store(ip, uid)
				d.OldUserOnline.Delete(ip)
				return d.getSpeedBucket(email), false
			}
		}
	}

	// 4. 判定是否超限
	aliveIp := 0
	if v, ok := d.AliveList[uid]; ok {
		aliveIp = v
	}
	if deviceLimit > 0 && aliveIp >= deviceLimit {
		return nil, true
	}

	// 5. 记录新 IP 并返回限速桶
	userIPMap.Store(ip, uid)
	return d.getSpeedBucket(email), false
}

// GetOnlineDevice 执行上报并切换缓冲
func (d *InboundInfo) GetOnlineDevice() []api.OnlineUser {
	var onlineUser []api.OnlineUser
	d.OldUserOnline = new(sync.Map) 

	d.UserOnlineIP.Range(func(key, value interface{}) bool {
		email := key.(string)
		ipMap := value.(*sync.Map)
		ipMap.Range(func(ip, uid interface{}) bool {
			onlineUser = append(onlineUser, api.OnlineUser{
				UID: uid.(int),
				IP:  ip.(string),
			})
			d.OldUserOnline.Store(ip, uid) // 存入旧列表以备下个周期“捞回”
			return true
		})
		return true
	})

	d.UserOnlineIP = new(sync.Map) // 清空当前统计
	return onlineUser
}
type Limiter struct {
	InboundInfo *sync.Map // Key: Tag, Value: *InboundInfo
}

func New() *Limiter {
	return &Limiter{
		InboundInfo: new(sync.Map),
	}
}

func (l *Limiter) AddInboundLimiter(tag string, nodeSpeedLimit uint64, userList *[]api.UserInfo, globalLimit *GlobalDeviceLimitConfig) error {
	inboundInfo := &InboundInfo{
		Tag:            tag,
		NodeSpeedLimit: nodeSpeedLimit,
		BucketHub:      new(sync.Map),
		UserOnlineIP:   new(sync.Map),
		OldUserOnline:  new(sync.Map),
	}

	if globalLimit != nil && globalLimit.Enable {
		inboundInfo.GlobalLimit.config = globalLimit

		// init local store
		gs := goCacheStore.NewGoCache(goCache.New(time.Duration(globalLimit.Expiry)*time.Second, 1*time.Minute))

		// init redis store
		rs := redisStore.NewRedis(redis.NewClient(
			&redis.Options{
				Network:  globalLimit.RedisNetwork,
				Addr:     globalLimit.RedisAddr,
				Username: globalLimit.RedisUsername,
				Password: globalLimit.RedisPassword,
				DB:       globalLimit.RedisDB,
			}),
			store.WithExpiration(time.Duration(globalLimit.Expiry)*time.Second))

		// init chained cache. First use local go-cache, if go-cache is nil, then use redis cache
		cacheManager := cache.NewChain[any](
			cache.New[any](gs), // go-cache is priority
			cache.New[any](rs),
		)
		inboundInfo.GlobalLimit.globalOnlineIP = marshaler.New(cacheManager)
	}

	userMap := new(sync.Map)
	for _, u := range *userList {
		userMap.Store(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID), UserInfo{
			UID:         u.UID,
			SpeedLimit:  u.SpeedLimit,
			DeviceLimit: u.DeviceLimit,
		})
	}
	inboundInfo.UserInfo = userMap
	l.InboundInfo.Store(tag, inboundInfo) // Replace the old inbound info
	return nil
}

func (l *Limiter) UpdateInboundLimiter(tag string, updatedUserList *[]api.UserInfo) error {
	if value, ok := l.InboundInfo.Load(tag); ok {
		inboundInfo := value.(*InboundInfo)
		// Update User info
		for _, u := range *updatedUserList {
			inboundInfo.UserInfo.Store(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID), UserInfo{
				UID:         u.UID,
				SpeedLimit:  u.SpeedLimit,
				DeviceLimit: u.DeviceLimit,
			})
			// Update old limiter bucket
			limit := determineRate(inboundInfo.NodeSpeedLimit, u.SpeedLimit)
			if limit > 0 {
				if bucket, ok := inboundInfo.BucketHub.Load(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID)); ok {
					limiter := bucket.(*rate.Limiter)
					limiter.SetLimit(rate.Limit(limit))
					limiter.SetBurst(int(limit))
				}
			} else {
				inboundInfo.BucketHub.Delete(fmt.Sprintf("%s|%s|%d", tag, u.Email, u.UID))
			}
		}
	} else {
		return fmt.Errorf("no such inbound in limiter: %s", tag)
	}
	return nil
}

func (l *Limiter) DeleteInboundLimiter(tag string) error {
	l.InboundInfo.Delete(tag)
	return nil
}

func (l *Limiter) GetOnlineDevice(tag string) (*[]api.OnlineUser, error) {
	var onlineUser []api.OnlineUser

	if value, ok := l.InboundInfo.Load(tag); ok {
		inboundInfo := value.(*InboundInfo)
		// Clear Speed Limiter bucket for users who are not online
		inboundInfo.BucketHub.Range(func(key, value interface{}) bool {
			email := key.(string)
			if _, exists := inboundInfo.UserOnlineIP.Load(email); !exists {
				inboundInfo.BucketHub.Delete(email)
			}
			return true
		})
		inboundInfo.UserOnlineIP.Range(func(key, value interface{}) bool {
			email := key.(string)
			ipMap := value.(*sync.Map)
			ipMap.Range(func(key, value interface{}) bool {
				uid := value.(int)
				ip := key.(string)
				inboundInfo.OldUserOnline.Store(ip, uid)
				onlineUser = append(onlineUser, api.OnlineUser{UID: uid, IP: ip})
				return true
			})
			inboundInfo.UserOnlineIP.Delete(email) // Reset online device
			return true
		})
	} else {
		return nil, fmt.Errorf("no such inbound in limiter: %s", tag)
	}

	return &onlineUser, nil
}

func (l *Limiter) GetUserBucket(tag string, email string, ip string, isSourceTCP bool) (limiter *rate.Limiter, SpeedLimit bool, Reject bool) {
	if value, ok := l.InboundInfo.Load(tag); ok {
		var (
			userLimit        uint64 = 0
			deviceLimit, uid int
		)

		inboundInfo := value.(*InboundInfo)
		nodeLimit := inboundInfo.NodeSpeedLimit

		if v, ok := inboundInfo.UserInfo.Load(email); ok {
			u := v.(UserInfo)
			uid = u.UID
			userLimit = u.SpeedLimit
			deviceLimit = u.DeviceLimit
		}

		// Local device limit, only for TCP connection
		if isSourceTCP {
			ipMap := new(sync.Map)
			ipMap.Store(ip, uid)
			aliveIp := inboundInfo.AliveList[uid]
			// If any device is online
			if v, ok := inboundInfo.UserOnlineIP.LoadOrStore(email, ipMap); ok {
				ipMap := v.(*sync.Map)
				// If this is a new ip
				if _, ok := ipMap.LoadOrStore(ip, uid); !ok {
					if deviceLimit > 0 {
						if deviceLimit <= aliveIp {
							ipMap.Delete(ip)
							return nil, false, true
						}
					}
				}
			} else if v, ok := inboundInfo.OldUserOnline.Load(ip); ok {
				if v.(int) == uid {
					inboundInfo.OldUserOnline.Delete(ip)
				}
			} else {
				if deviceLimit > 0 {
					if deviceLimit <= aliveIp {
						inboundInfo.UserOnlineIP.Delete(email)
						return nil, false, true
					}
				}
			}
		}

		// GlobalLimit
		if inboundInfo.GlobalLimit.config != nil && inboundInfo.GlobalLimit.config.Enable {
			if reject := globalLimit(inboundInfo, email, uid, ip, deviceLimit); reject {
				return nil, false, true
			}
		}

		// Speed limit
		limit := determineRate(nodeLimit, userLimit) // Determine the speed limit rate
		if limit > 0 {
			limiter := rate.NewLimiter(rate.Limit(limit), int(limit)) // Byte/s
			if v, ok := inboundInfo.BucketHub.LoadOrStore(email, limiter); ok {
				bucket := v.(*rate.Limiter)
				return bucket, true, false
			} else {
				return limiter, true, false
			}
		} else {
			return nil, false, false
		}
	} else {
		log.Error("Get Inbound Limiter information failed")
		return nil, false, false
	}
}

// Global device limit
func globalLimit(inboundInfo *InboundInfo, email string, uid int, ip string, deviceLimit int) bool {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(inboundInfo.GlobalLimit.config.Timeout)*time.Second)
	defer cancel()

	// reformat email for unique key
	uniqueKey := strings.Replace(email, inboundInfo.Tag, strconv.Itoa(deviceLimit), 1)

	v, err := inboundInfo.GlobalLimit.globalOnlineIP.Get(ctx, uniqueKey, new(map[string]int))
	if err != nil {
		if _, ok := err.(*store.NotFound); ok {
			// If the email is a new device
			go pushIP(inboundInfo, uniqueKey, &map[string]int{ip: uid})
		} else {
			log.Error("cache service", err)
		}
		return false
	}

	ipMap := v.(*map[string]int)
	// Reject device reach limit directly
	if deviceLimit > 0 && len(*ipMap) > deviceLimit {
		return true
	}

	// If the ip is not in cache
	if _, ok := (*ipMap)[ip]; !ok {
		(*ipMap)[ip] = uid
		go pushIP(inboundInfo, uniqueKey, ipMap)
	}

	return false
}

// push the ip to cache
func pushIP(inboundInfo *InboundInfo, uniqueKey string, ipMap *map[string]int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(inboundInfo.GlobalLimit.config.Timeout)*time.Second)
	defer cancel()

	if err := inboundInfo.GlobalLimit.globalOnlineIP.Set(ctx, uniqueKey, ipMap); err != nil {
		log.Error("cache service", err)
	}
}

// determineRate returns the minimum non-zero rate
func determineRate(nodeLimit, userLimit uint64) (limit uint64) {
	if nodeLimit == 0 || userLimit == 0 {
		if nodeLimit > userLimit {
			return nodeLimit
		} else if nodeLimit < userLimit {
			return userLimit
		} else {
			return 0
		}
	} else {
		if nodeLimit > userLimit {
			return userLimit
		} else if nodeLimit < userLimit {
			return nodeLimit
		} else {
			return nodeLimit
		}
	}
}
