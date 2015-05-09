package pin

import (
	ds "github.com/ipfs/go-ipfs/Godeps/_workspace/src/github.com/jbenet/go-datastore"
	"github.com/ipfs/go-ipfs/util"
)

type indirectPin struct {
	refCounts map[util.Key]uint64
}

func newIndirectPin() *indirectPin {
	return &indirectPin{
		refCounts: make(map[util.Key]uint64),
	}
}

func loadIndirPin(d ds.Datastore, k ds.Key) (*indirectPin, error) {
	var rcStore map[string]uint64
	err := loadSet(d, k, &rcStore)
	if err != nil {
		return nil, err
	}

	refcnt := make(map[util.Key]uint64)
	var keys []util.Key
	for encK, v := range rcStore {
		if v > 0 {
			k := util.B58KeyDecode(encK)
			keys = append(keys, k)
			refcnt[k] = v
		}
	}
	// log.Debugf("indirPin keys: %#v", keys)

	return &indirectPin{refCounts: refcnt}, nil
}

func storeIndirPin(d ds.Datastore, k ds.Key, p *indirectPin) error {

	rcStore := map[string]uint64{}
	for k, v := range p.refCounts {
		rcStore[util.B58KeyEncode(k)] = v
	}
	return storeSet(d, k, rcStore)
}

func (i *indirectPin) Increment(k util.Key) {
	i.refCounts[k]++
}

func (i *indirectPin) Decrement(k util.Key) {
	i.refCounts[k]--
	if i.refCounts[k] == 0 {
		delete(i.refCounts, k)
	}
}

func (i *indirectPin) HasKey(k util.Key) bool {
	_, found := i.refCounts[k]
	return found
}

func (i *indirectPin) GetRefs() map[util.Key]uint64 {
	return i.refCounts
}
