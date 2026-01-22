package rados

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"

	"github.com/ceph/go-ceph/rados"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type radosCreator struct{}

const (
	radosType = "rados.type"
	poolName  = "rados.pool"
	radosNum  = "rados.nums"

	OmapType  = "omap"
	XattrType = "xattr"
)

type radosDB struct {
	p        *properties.Properties
	tp       string
	shard    uint32
	pool     string
	workers  int
	maxValue int
	conn     *rados.Conn
	bufPool  *util.BufPool
	r        *util.RowCodec
	free     chan *rados.IOContext
}

func (r *radosDB) InitPool() error {
	names, err := r.conn.ListPools()
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == r.pool {
			return nil
		}
	}
	return r.conn.MakePool(r.pool)
}

func (r radosCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	conn, err := rados.NewConnWithUser("admin")
	if err != nil {
		return nil, err
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		return nil, err
	}
	err = conn.Connect()
	if err != nil {
		return nil, err
	}
	tp := p.GetString(radosType, OmapType)
	if tp != XattrType {
		tp = OmapType
	}
	pool := p.GetString(poolName, "foo")
	workers := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	recordCount := p.GetInt64(prop.RecordCount, 0)
	num := p.GetInt64(radosNum, 10000)
	shard := (recordCount + num - 1) / num
	if shard <= 0 {
		return nil, errors.New("not found record-count")
	}

	fieldCount := p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	maxValue := fieldCount * fieldLength * 2
	log.Printf("type: %s, pool: %s, max-value: %d, shard: %d, workers: %d\n", tp, pool, maxValue, shard, workers)
	db := &radosDB{
		p:        p,
		tp:       tp,
		conn:     conn,
		pool:     pool,
		maxValue: int(maxValue),
		shard:    uint32(shard),
		free:     make(chan *rados.IOContext, workers),
		r:        util.NewRowCodec(p),
		bufPool:  util.NewBufPool(),
	}
	err = db.InitPool()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (r *radosDB) Close() error {
	r.conn.Shutdown()
	return nil
}

func (db *radosDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *radosDB) CleanupThread(ctx context.Context) {
}

func (r *radosDB) NewContext() (*rados.IOContext, error) {
	select {
	case ctx := <-r.free:
		return ctx, nil
	default:
		ctx, err := r.conn.OpenIOContext(r.pool)
		if err == nil {
			_ = ctx.SetPoolFullTry()
		}
		return ctx, err
	}
}

func (r *radosDB) Release(ctx *rados.IOContext, err error) {
	if err != nil {
		ctx.Destroy()
		return
	}
	select {
	case r.free <- ctx:
	default:
		ctx.Destroy()
	}
}

func (r *radosDB) getRowKey(table string, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

func (r *radosDB) crcHashMod(table, key string) string {
	keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)
	return fmt.Sprintf("%s/%d", table, keyCrc%r.shard)
}

func (r *radosDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := r.getRowKey(table, key)

	buf := r.bufPool.Get()
	defer r.bufPool.Put(buf)

	buf, err := r.r.Encode(buf, values)
	if err != nil {
		log.Printf("err: %s\n", err)
		return err
	}
	ioctx, err := r.NewContext()
	defer r.Release(ioctx, err)
	if err != nil {
		log.Printf("err: %s\n", err)
		return err
	}
	oid := r.crcHashMod(table, key)
	switch r.tp {
	case XattrType:
		err = ioctx.SetXattr(oid, rowKey, buf)
	default:
		err = ioctx.SetOmap(oid, map[string][]byte{rowKey: buf})
	}
	if err != nil {
		log.Printf("err: %s\n", err)
	}
	return err
}

func (r *radosDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	rowKey := r.getRowKey(table, key)
	ioctx, err := r.NewContext()
	defer r.Release(ioctx, err)
	if err != nil {
		log.Printf("err: %s\n", err)
		return nil, err
	}
	oid := r.crcHashMod(table, key)
	var value []byte
	switch r.tp {
	case XattrType:
		out := make([]byte, r.maxValue)
		n, err := ioctx.GetXattr(oid, rowKey, out)
		if err != nil {
			log.Printf("err: %s\n", err)
			return nil, err
		}
		value = out[:n]
	default:
		op := rados.CreateReadOp()
		defer op.Release()
		kvStep := op.GetOmapValuesByKeys([]string{rowKey})
		err = op.Operate(ioctx, oid, rados.OperationNoFlag)
		kv, err := kvStep.Next()
		if err != nil {
			log.Printf("err: %s\n", err)
			return nil, err
		}

		if kv == nil {
			return nil, nil
		}
		value = kv.Value
	}
	return r.r.Decode(value, fields)
}

func (r *radosDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

func (r *radosDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return nil
}

func (r *radosDB) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func init() {
	ycsb.RegisterDBCreator("rados", radosCreator{})
}
