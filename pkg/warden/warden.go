package warden

//type Warden struct {
//	connName string
//	db       *badger.DB
//}
//
//func NewWarden() *Warden {
//	return &Warden{}
//}
//
//func (w *Warden) Connect() (err error) {
//	w.db, err = badger.Open(badger.DefaultOptions("warden"))
//	return
//}
//
//func (w *Warden) WithConnectorName(name string) *Warden {
//	clone := *w
//	clone.connName = name
//	return &clone
//}
//
//func (w *Warden) SavePos() {
//	w.db.Update(func(txn *badger.Txn) error {
//		txn.Set()
//	})
//}
