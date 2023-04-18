package k8sclientkit

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type UnstructuredApplyResult struct {
	Gvk          schema.GroupVersionKind
	Success      bool
	Error        error
	ResultObject *unstructured.Unstructured
}
