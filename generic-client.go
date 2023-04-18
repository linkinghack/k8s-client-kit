package k8sclientkit

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	AuthTypeToken           = "TOKEN"
	AuthTypeKubeConfigBytes = "KUBECONFIG_BYTES"
	AuthTypeInCluster       = "IN_CLUSTER"
)

// GenericK8sClient 用于与一个指定的Kubernetes APIServer通信。
// 内置基本标准类型客户端和动态类型客户端（支持任意GVK访问）
type GenericK8sClient struct {
	TargetK8sApiServerId string
	// 创建方式类型: ByToken, ByKubeConfigBytes
	AuthType string

	// 客户端配置KubeConfig
	kubeConfig *clientcmdapi.Config
	restConfig *rest.Config

	// controller-runtime Cluster 超级客户端工具实现
	runtimeCluster cluster.Cluster
	mgrCtx         context.Context
	stopMgr        context.CancelFunc

	// 用于标准对象的客户端单例
	standardClient *kubernetes.Clientset
	// 用于通用对象的客户端单例
	dynamicClient dynamic.Interface

	// scheme register lock
	schemeLock *sync.Mutex
}

func (c *GenericK8sClient) GetDynamicClient() dynamic.Interface {
	return c.dynamicClient
}
func (c *GenericK8sClient) GetStandardClient() *kubernetes.Clientset {
	return c.standardClient
}
func (c *GenericK8sClient) GetRuntimeCluster() cluster.Cluster {
	return c.runtimeCluster
}

// Add a new api-group scheme to this client
// `gv` 和 `addSchemeFunc` 必须来自同一API package
func (c *GenericK8sClient) AddScheme(gv *schema.GroupVersion, addSchemeFunc func(s *runtime.Scheme) error) {
	c.schemeLock.Lock()
	defer c.schemeLock.Unlock()

	if !c.runtimeCluster.GetScheme().IsVersionRegistered(*gv) {
		addSchemeFunc(c.runtimeCluster.GetScheme())
	}
}

// 启动内置manager watcher
func (c *GenericK8sClient) Start() {
	if c.runtimeCluster == nil {
		return
	}
	c.runtimeCluster.Start(c.mgrCtx)
}
func (c *GenericK8sClient) Stop() {
	c.stopMgr()
}

// newGenericK8sClientWithKubeConfigObj 使用指定的kube config实例创建GenericK8sClient
func newGenericK8sClientWithKubeConfigObj(id, authType string, config *clientcmdapi.Config) (*GenericK8sClient, error) {

	// 构建rest client config
	clientConfig := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{})
	restClientConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, errors.Wrap(err, "使用clientcmdapi.Config方式构建rest client config失败")
	}

	// dynamic client
	dc, err := dynamic.NewForConfig(restClientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "创建dynamic client失败")
	}

	// standard client
	sc, err := kubernetes.NewForConfig(restClientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "创建standard client失败")
	}

	// controller-runtime Client
	// mgr, err := manager.New(restClientConfig, manager.Options{})
	// if err != nil {
	// 	logger.WithError(err).Debug("无法创建runtime-controller.Manager")
	// 	return nil, errors.Wrap(err, "创建runtime-controller.Manager失败")
	// }

	opt := manager.Options{}
	clusterCli, err := cluster.New(restClientConfig, func(clusterOptions *cluster.Options) {
		clusterOptions.Scheme = opt.Scheme
		clusterOptions.MapperProvider = opt.MapperProvider
		clusterOptions.Logger = opt.Logger
		clusterOptions.SyncPeriod = opt.SyncPeriod
		clusterOptions.Namespace = opt.Namespace
		clusterOptions.NewCache = opt.NewCache
		clusterOptions.NewClient = opt.NewClient
		clusterOptions.ClientDisableCacheFor = opt.ClientDisableCacheFor
		clusterOptions.DryRunClient = opt.DryRunClient
	})
	if err != nil {
		return nil, errors.Wrap(err, "创建runtime-controller.Cluster")
	}

	mgrCtx, stop := context.WithCancel(context.Background()) // runtime clusterCli 后台管理context
	return &GenericK8sClient{
		TargetK8sApiServerId: id,
		AuthType:             authType,
		kubeConfig:           config,
		restConfig:           restClientConfig,
		standardClient:       sc,
		dynamicClient:        dc,
		runtimeCluster:       clusterCli,
		mgrCtx:               mgrCtx,
		stopMgr:              stop,
		schemeLock:           &sync.Mutex{},
	}, nil
}

// newGenericK8sClientWithRestConfig 使用rest client config 创建K8sClient
func newGenericK8sClientWithRestConfig(id, authType string, config *rest.Config) (*GenericK8sClient, error) {
	// dynamic client
	dc, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "创建dynamic client失败")
	}

	// standard client
	sc, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "创建standard client失败")
	}

	_, err = sc.ServerVersion()
	if err != nil {
		return nil, errors.Wrap(err, "无法连接到集群")
	}

	// controller-runtime Client
	// mgr, err := manager.New(config, manager.Options{})
	// if err != nil {
	// 	logger.WithError(err).Debug("无法创建runtime-controller.Manager")
	// 	return nil, errors.Wrap(err, "创建runtime-controller.Manager失败")
	// }

	opt := manager.Options{}
	clusterCli, err := cluster.New(config, func(clusterOptions *cluster.Options) {
		clusterOptions.Scheme = opt.Scheme
		clusterOptions.MapperProvider = opt.MapperProvider
		clusterOptions.Logger = opt.Logger
		clusterOptions.SyncPeriod = opt.SyncPeriod
		clusterOptions.Namespace = opt.Namespace
		clusterOptions.NewCache = opt.NewCache
		clusterOptions.NewClient = opt.NewClient
		clusterOptions.ClientDisableCacheFor = opt.ClientDisableCacheFor
		clusterOptions.DryRunClient = opt.DryRunClient
	})
	if err != nil {
		return nil, errors.Wrap(err, "创建runtime-controller.Cluster")
	}

	mgrCtx, stop := context.WithCancel(context.Background())
	return &GenericK8sClient{
		TargetK8sApiServerId: id,
		AuthType:             authType,
		kubeConfig:           nil,
		restConfig:           config,
		standardClient:       sc,
		dynamicClient:        dc,
		mgrCtx:               mgrCtx,
		stopMgr:              stop,
		runtimeCluster:       clusterCli,
		schemeLock:           &sync.Mutex{},
	}, nil
}

// NewGenericK8sClientWithToken 使用目标集群的ApiServer url和具有一定访问权限的bearer token来构建一个generic client.
// token 可以通过创建ServiceAccount并获取对应的Secret来得到(从v1.24开始需要开启相关的特性门控才会自动创建Secret).
//
//	apiServerUrl: 目标apiserver的访问地址，如`https://cluster-1.dc1.example.com:6443`, 或`https://10.2.0.121:6443`
//	optionalTLSServerName: 用于校验服务端证书是否与此名称一致，默认同APIServerURL保持一致; 同时也影响TLS SNI, 在client hello 中将传递给server
//	caPem: PEM编码的信任CA，应该设置为目标APIServer的CA证书
func NewGenericK8sClientWithToken(id, apiServerUrl, token string, caPem []byte, optionalTLSServerName string, skipTLSVerify bool) (*GenericK8sClient, error) {
	// 准备kubeconfig模板
	config := clientcmdapi.NewConfig()
	config.Clusters["cluster"] = &clientcmdapi.Cluster{
		Server:                   apiServerUrl,
		TLSServerName:            optionalTLSServerName,
		InsecureSkipTLSVerify:    skipTLSVerify,
		CertificateAuthorityData: caPem,
	}
	config.AuthInfos["cluster"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: nil,
		ClientKeyData:         nil,
		Token:                 token,
	}
	config.Contexts["cluster"] = &clientcmdapi.Context{
		Cluster:   "cluster",
		AuthInfo:  "cluster",
		Namespace: "default",
	}
	config.CurrentContext = "cluster"

	return newGenericK8sClientWithKubeConfigObj(id, AuthTypeToken, config)
}

func NewGenericK8sClientWithSecretDir(id, authSecretDir, apiServerUrl, sni string) (*GenericK8sClient, error) {
	// 读取目标目录中认证信息
	// 通常应该包含几个文本文件：ca.crt, namespace, token
	dir := strings.TrimSuffix(authSecretDir, "/")
	caCert, err := os.ReadFile(dir + "/ca.crt")
	if err != nil {
		return nil, errors.Wrap(err, "无法读取CA Cert文件:"+dir+"/ca.crt")
	}

	token, err := os.ReadFile(dir + "/token")
	if err != nil {
		return nil, errors.Wrap(err, "无法读取Token文件:"+dir+"token")
	}

	return NewGenericK8sClientWithToken(id, apiServerUrl, string(token), caCert, sni, false)
}

// NewGenericK8sClientWithKubeConfigBytes 使用指定的KubeConfig bytes创建K8sClient
//
//	param: overrideServerName string 可选的指定APIServer TLS 域名SNI
//
// Kubeconfig中cluster server支持使用IP地址端口方式指定可保证正确访问到的路由，若目标ApiServer在TLS SNI代理服务器之后，
// 可以通过指定overrideServerName来使代理服务器正常工作。
// 另外可选的方式是在部署k8s-provisioner时外部环境中解决域名解析问题，则可以直接在kubeconfig中使用域名方式指定APIServer地址。
func NewGenericK8sClientWithKubeConfigBytes(id string, kubeConfig []byte, overrideSNIServerName string) (*GenericK8sClient, error) {
	clientConfig, err := clientcmd.NewClientConfigFromBytes(kubeConfig)
	if err != nil {
		return nil, errors.Wrap(err, "无法使用指定KubeConfig bytes创建client config")
	}

	config, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, errors.Wrap(err, "无法使用Kubeconfig bytes生成的clientcmd.ClientConfig创建rest client config")
	}
	if len(overrideSNIServerName) > 1 {
		// 覆盖TLS ServerName
		config.ServerName = overrideSNIServerName
	}
	return newGenericK8sClientWithRestConfig(id, AuthTypeKubeConfigBytes, config)
}

// NewGenericK8sClientInCluster 使用当前集群SA创建K8sClient. 仅在Kubernetes集群内部署时可用
func NewGenericK8sClientInCluster(id string) (*GenericK8sClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "无法从使用集群内配置(挂载的ServiceAccount token)构建rest client")
	}

	return newGenericK8sClientWithRestConfig(id, AuthTypeInCluster, config)
}
