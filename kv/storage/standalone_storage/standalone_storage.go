package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	enginnes *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvEngine := engine_util.CreateDB(conf.DBPath, conf.Raft)
	//raftEngine := engine_util.CreateDB(conf.DBPath, conf.Raft)
	return &StandAloneStorage{
		enginnes: engine_util.NewEngines(kvEngine, nil, conf.DBPath, conf.DBPath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.enginnes.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := NewStandAloneReader(s.enginnes.Kv)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if len(batch) > 0 {
		err := s.enginnes.Kv.Update(func(txn *badger.Txn) error {
			for _, modify := range batch {
				var err1 error
				switch modify.Data.(type) {
				case storage.Put:
					put := modify.Data.(storage.Put)
					err1 = txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
				case storage.Delete:
					deleteS := modify.Data.(storage.Delete)
					err1 = txn.Delete(engine_util.KeyWithCF(deleteS.Cf, deleteS.Key))
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewStandAloneReader(db *badger.DB) *StandAloneReader {
	return &StandAloneReader{
		txn: db.NewTransaction(false),
	}
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil && err.Error() == badger.ErrKeyNotFound.Error() {
		value = nil
		err = nil
	}
	return value, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
}
