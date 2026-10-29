package sql

import (
	p "go.temporal.io/server/common/persistence"
)

var (
	_ p.StoreFactsReceiver = (*Factory)(nil)
	_ p.StoreFactsReceiver = (*DbConn)(nil)
)

// SetStoreFacts passes the sink to the plugin's database, now or when it's created.
func (f *Factory) SetStoreFacts(facts p.StoreFacts) {
	f.mainDBConn.SetStoreFacts(facts)
}

// SetStoreFacts keeps the sink for the database this connection creates, and passes it
// on if the database already exists.
func (c *DbConn) SetStoreFacts(facts p.StoreFacts) {
	c.Lock()
	defer c.Unlock()
	c.storeFacts = facts
	c.applyStoreFacts()
}

// applyStoreFacts passes the sink to a plugin database that reports facts. Callers hold
// the lock.
func (c *DbConn) applyStoreFacts() {
	if c.DB == nil || c.storeFacts == nil {
		return
	}
	if receiver, ok := c.DB.(p.StoreFactsReceiver); ok {
		receiver.SetStoreFacts(c.storeFacts)
	}
}
