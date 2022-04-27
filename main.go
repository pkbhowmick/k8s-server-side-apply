package main

import (
	"context"
	"fmt"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/klogr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

func NewClient() (client.Client, error) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)

	ctrl.SetLogger(klogr.New())
	cfg := ctrl.GetConfigOrDie()
	cfg.QPS = 100
	cfg.Burst = 100

	mapper, err := apiutil.NewDynamicRESTMapper(cfg)
	if err != nil {
		return nil, err
	}

	return client.New(cfg, client.Options{
		Scheme: scheme,
		Mapper: mapper,
		//Opts: client.WarningHandlerOptions{
		//	SuppressWarnings:   false,
		//	AllowDuplicateLogs: false,
		//},
	})
}

func main() {
	kc, err := NewClient()
	if err != nil {
		panic(err)
	}

	key := client.ObjectKey{Namespace: core.NamespaceDefault, Name: "test-secret"}
	
	secret := core.Secret{
		TypeMeta:   metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: core.SchemeGroupVersion.String(),
		}, 
		ObjectMeta: metav1.ObjectMeta{
			Name: key.Name,
			Namespace: key.Namespace,
		},
		StringData: map[string]string{
			"username": "admin",
			"password": "admin",
		},
		Type:       core.SecretTypeOpaque,
	}

	err = kc.Patch(context.TODO(), &secret, client.Apply, client.FieldOwner("manager1"))
	if err != nil {
		panic(err)
	}
	klog.Info("created successfully with manager1 as a field manager")

	// getting the secret
	createdSecret := core.Secret{}
	err = kc.Get(context.TODO(), key, &createdSecret)
	if err != nil {
		panic(err)
	}
	fmt.Println(createdSecret)

	upSecret := secret.DeepCopy()
	upSecret.StringData = map[string]string{"key":"val"}

	// expecting a conflict with different manager "manager2"
	err = kc.Patch(context.TODO(), upSecret, client.Apply, client.FieldOwner("manager2"))
	if err != nil {
		klog.Errorf(err.Error())
	}

	// cleanup secret
	err = kc.Delete(context.TODO(), upSecret)
	if err != nil {
		panic(err)
	}
	klog.Info("secret deleted")
}
