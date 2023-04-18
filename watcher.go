package k8sclientkit

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	WatcherTypeDynamic  = "Dynamic"
	WatcherTypeStandard = "Standard"
)

type K8sResourceWatcher struct {
	Gvr       schema.GroupVersionResource
	Namespace string
	queue     workqueue.RateLimitingInterface
	informer  informers.GenericInformer
	lister    cache.GenericLister

	stop chan struct{}
}

// NewDynamicWatcher 创建一个新的通用资源对象watcher
// 指定目标对象
func NewDynamicWatcher(client dynamic.Interface, resource schema.GroupVersionResource, namespace string, resync time.Duration, indexers cache.Indexers, listOptionsFunc dynamicinformer.TweakListOptionsFunc) *K8sResourceWatcher {
	defaultIndexers := cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}
	if len(indexers) > 0 {
		for k, v := range indexers {
			defaultIndexers[k] = v
		}
	}

	informer := dynamicinformer.NewFilteredDynamicInformer(client, resource, namespace, resync, defaultIndexers, listOptionsFunc)

	// informer := informerFactory.ForResource(resource).Informer()
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// // 默认index 基于namespace
	// informer.AddIndexers(cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})

	return &K8sResourceWatcher{
		Namespace: namespace,
		queue:     queue,
		informer:  informer,
		stop:      make(chan struct{}),
		lister:    informer.Lister(),
	}
}

func (w *K8sResourceWatcher) GetObjectsInNamespace(namespace string) {
	w.informer.Lister().ByNamespace(namespace).List(labels.Everything())
}

// GetObject 从informer存储中获取指定namespace/name的对象
func (w *K8sResourceWatcher) GetObject(namespace, name string) (obj interface{}, exists bool, err error) {
	// TIP: SharedIndexInformer中使用cache.MetaNamespaceKeyFunc 将namespace/name作为主索引的key
	key := name
	if len(namespace) > 0 {
		key = fmt.Sprintf("%s/%s", namespace, name)
	}

	return w.informer.Informer().GetIndexer().Get(key)
}

func (w *K8sResourceWatcher) Start() {
	w.informer.Informer().Run(w.stop)
}

func (w *K8sResourceWatcher) Stop() {
	w.stop <- struct{}{}
}

func (w *K8sResourceWatcher) AddEventHandler(addHandler, delHandler func(obj interface{}), updateHandler func(oldObj, newObj interface{})) {
	// 事件处理支持
	// w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
	// 	AddFunc: func(obj interface{}) {
	// 		key, err := cache.MetaNamespaceKeyFunc(obj)
	// 		if err == nil {
	// 			queue.Add(key)
	// 		}
	// 	},
	// 	UpdateFunc: func(oldObj, newObj interface{}) {
	// 		// TODO: 定义专门的event类型，将事件类型和old/new object包括其中
	// 		key, err := cache.MetaNamespaceKeyFunc(newObj)
	// 		if err == nil {
	// 			queue.Add(key)
	// 		}
	// 	},
	// 	DeleteFunc: func(obj interface{}) {
	// 		key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	// 		if err == nil {
	// 			queue.Add(key)
	// 		}
	// 	},
	// })

	w.informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    addHandler,
		UpdateFunc: updateHandler,
		DeleteFunc: delHandler,
	})
}
