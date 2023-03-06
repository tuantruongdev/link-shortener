package mysql

import (
	"context"
	"fmt"
	"github.com/kellegous/go/internal"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
	"time"
)

type Backend struct {
	db  *gorm.DB
	lck sync.Mutex
	id  uint64
}

func (backend *Backend) Close() error {
	return nil
}

type SqlRoute struct {
	Id   int    `json:"id" gorm:"id"`
	Name string `json:"name" gorm:"name"`
	Url  string `json:"url" gorm:"url"`
	Time int64  `json:"time" gorm:"time"`
}

func (SqlRoute) TableName() string { return "route" }

func New(connection string) (*Backend, error) {
	db, error := gorm.Open(mysql.Open(connection), &gorm.Config{})
	if error != nil {
		return nil, error
	}
	backend := Backend{db: db, lck: sync.Mutex{}}
	var currentMax uint64
	err := backend.db.Model(SqlRoute{}).Select("max(id)").Scan(&currentMax).Error
	if err != nil {
		fmt.Println(err)
		backend.id = 0
		//	fmt.Println("current max=", currentMax)
		return &backend, nil
	}
	backend.id = currentMax
	//	fmt.Println("current max=", currentMax)
	return &backend, nil
}

func (backend *Backend) isExits(key string) bool {
	var count int64
	err := backend.db.Model(SqlRoute{}).Where("name = ?", key).Count(&count).Error
	if err != nil {
		fmt.Println(err)
		return false
	}
	return count > 0
}

// Put stores a new shortcut in the data store.
func (backend *Backend) Put(ctx context.Context, key string, rt *internal.Route) error {
	sqlRoute := toSqlRoute(key, rt)
	//fmt.Printf("%+v", sqlRoute)
	isExits := backend.isExits(key)
	if isExits {
		return backend.Update(ctx, key, sqlRoute)
	} else {
		err := backend.db.Create(&sqlRoute).Error
		//	backend.db.Exec("INSERT INTO route(id,name,url,time) VALUES(0,\"truong\",\"gmail.com\",123)")
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Printf("added key %s with url %s at %s \n", sqlRoute.Name, sqlRoute.Url, time.Unix(sqlRoute.Time, 0))
		return err
	}
}

func (backend *Backend) Update(ctx context.Context, key string, rt *SqlRoute) error {
	return backend.db.Model(SqlRoute{}).Where("name = ?", key).Update("url", rt.Url).Update("time", time.Unix(rt.Time, 0)).Error
}

// Get retrieves a shortcut from the data store.
func (backend *Backend) Get(ctx context.Context, key string) (*internal.Route, error) {
	var sqlRoute = SqlRoute{}
	err := backend.db.Where("name=?", key).First(&sqlRoute).Error
	internalRoute := toInternalRoute(key, sqlRoute)
	//	fmt.Printf("%+v", internalRoute)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return internalRoute, err
}

// Del removes an existing shortcut from the data store.
func (backend *Backend) Del(ctx context.Context, key string) error {
	err := backend.db.Where("name=?", key).Delete(SqlRoute{}).Error
	return err
}

// List all routes in an iterator, starting with the key prefix of start (which can also be nil).
func (backend *Backend) List(ctx context.Context, start string) (internal.RouteIterator, error) {
	//return &RouteIterator{
	//	it: backend.db(&util.Range{
	//		Start: []byte(start),
	//		Limit: nil,
	//	}, nil),
	//}, nil
	//todo()
	return nil, nil
}

// GetAll gets everything in the db to dump it out for backup purposes
func (backend *Backend) GetAll(ctx context.Context) (map[string]internal.Route, error) {
	routeMap := map[string]internal.Route{}
	var routeArr []SqlRoute
	err := backend.db. /*.Where("id not null")*/ Find(&routeArr).Error
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	for i := 0; i < len(routeArr); i++ {
		item := routeArr[i]
		routeMap[item.Name] = *toInternalRoute("", item)
	}
	return routeMap, err
}

// NextID generates the next numeric ID to be used for an auto-named shortcut.
func (backend *Backend) NextID(ctx context.Context) (uint64, error) {
	backend.lck.Lock()
	defer backend.lck.Unlock()

	backend.id++
	//fmt.Println(backend.id)
	//if err := commit(filepath.Join(backend.path, idLogFilename), backend.id); err != nil {
	//	return 0, err
	//}

	return backend.id, nil
}

func toSqlRoute(key string, rt *internal.Route) *SqlRoute {
	sqlRoute := SqlRoute{Name: key, Url: rt.URL, Time: rt.Time.Unix()}
	return &sqlRoute
}
func toInternalRoute(key string, rt SqlRoute) *internal.Route {
	internalRoute := internal.Route{URL: rt.Url, Time: time.Unix(rt.Time, 0)}
	return &internalRoute
}
