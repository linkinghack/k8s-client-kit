package k8sclientkit

import (
	"context"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (c *GenericK8sClient) ApplyUnstructuredObj(ctx context.Context, obj *unstructured.Unstructured, filedManager string) (*UnstructuredApplyResult, error) {
	err := c.GetRuntimeCluster().GetClient().Create(ctx, obj, &client.CreateOptions{FieldManager: filedManager})
	return &UnstructuredApplyResult{
		Gvk:          obj.GroupVersionKind(),
		Error:        err,
		Success:      err == nil,
		ResultObject: obj,
	}, err
}

func (c *GenericK8sClient) ApplyUnstructuredObjsBatch(ctx context.Context, objs []*unstructured.Unstructured, fieldManager string) (successfulResults []*UnstructuredApplyResult, failedResults []*UnstructuredApplyResult) {
	for _, obj := range objs {
		result, err := c.ApplyUnstructuredObj(ctx, obj, fieldManager)
		if err != nil {
			failedResults = append(failedResults, result)
		} else {
			successfulResults = append(successfulResults, result)
		}
	}

	return successfulResults, failedResults
}

func (c *GenericK8sClient) GvkToGvr(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	resourcesList, err := c.GetStandardClient().DiscoveryClient.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
	if err != nil {
		return schema.GroupVersionResource{}, err
	}

	for _, resource := range resourcesList.APIResources {
		if resource.Kind == gvk.Kind {
			return schema.GroupVersionResource{
				Group:    gvk.Group,
				Version:  gvk.Version,
				Resource: resource.Name,
			}, nil // 已找到
		}
	}

	return schema.GroupVersionResource{}, errors.New("未找到目标资源:" + gvk.String())
}
