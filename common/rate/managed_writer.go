package rate

import (
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

// ManagedWriter 包装了 buf.Writer，并在关闭时执行回调
type ManagedWriter struct {
	buf.Writer
	OnClose func()
}

// NewManagedWriter 创建一个带关闭回调的 Writer
func NewManagedWriter(writer buf.Writer, onClose func()) buf.Writer {
	return &ManagedWriter{
		Writer:  writer,
		OnClose: onClose,
	}
}

// Close 重写关闭逻辑，确保计数器减量且只执行一次
func (w *ManagedWriter) Close() error {
	if w.OnClose != nil {
		w.OnClose()
		w.OnClose = nil
	}
	return common.Close(w.Writer)
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	return w.Writer.WriteMultiBuffer(mb)
}
