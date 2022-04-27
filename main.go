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

	data := make(map[string]string)
	data["username"] = "admin"
	data["password"] = "admin"

	secret := core.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: core.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		StringData: data,
		Type: core.SecretTypeOpaque,
	}
	secret2 := secret.DeepCopy()

	var owner1, owner2 client.FieldOwner
	owner1 = "manager1"
	owner2 = "manager2"

	err = kc.Patch(context.TODO(), &secret, client.Apply, owner1)
	if err != nil {
		panic(err)
	}
	klog.Info("created successfully with owner1 as a field manager")

	// checking the created secret
	printSecret(kc, key)

	data["key"] = "val"
	secret2.StringData = data

	// expecting a conflict with different owner "owner2"
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner2)
	if err == nil {
		panic("expecting a conflict but no conflict occurred")
	}
	klog.Errorf("Conflict error: %v",err)

	// apply secret with current owner "owner1"
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner1)
	if err != nil {
		panic(err)
	}
	klog.Info("Secret is successfully updated")

	// check secret after successful updated
	printSecret(kc, key)

	// cleanup secret
	err = kc.Delete(context.TODO(), secret2)
	if err != nil {
		panic(err)
	}
	klog.Info("secret deleted")
}

func printSecret(kc client.Client, key client.ObjectKey) {
	createdSecret := core.Secret{}
	err := kc.Get(context.TODO(), key, &createdSecret)
	if err != nil {
		panic(err)
	}
	fmt.Println(createdSecret)
}
