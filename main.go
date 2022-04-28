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

	data := make(map[string][]byte)
	data["username"] = []byte("admin")
	data["password"] = []byte("admin")

	secret := core.Secret{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: core.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		Data: data,
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
	defer cleanSecret(kc, key)
	// checking the created secret
	printOwnerInfo(kc, key)

	data["username"] = []byte("changedUsername")
	secret2.Data = data
	// expecting a conflict with different owner "owner2"
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner2)
	printOwnerInfo(kc, key)
	if err == nil {
		panic("expecting a conflict but no conflict occurred")
	}
	klog.Errorf("Conflict error: %v", err)

	// apply secret with current owner "owner1"
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner1)
	if err != nil {
		panic(err)
	}
	klog.Info("Secret is successfully updated")
	// check secret after successful updated
	printOwnerInfo(kc, key)

	// ============= Conflicts Example #1 (Don't overwrite value, become shared manager) =============== //
	// make owner2 as shared owner of the fields without any modification in the data
	secret2.ManagedFields = nil
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner2)
	if err != nil {
		panic(err)
	}
	// check secret with shared ownership
	printOwnerInfo(kc, key)

	// ============= Conflicts Example #2 (Overwrite value, become sole manager) =============== //
	// now going to change data with owner2 should face conflict because now data is owned by both owner1 & owner2
	secret2.ManagedFields = nil
	data["username"] = []byte("none")
	secret2.Data = data
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner2)
	if err == nil {
		panic("expecting a conflict but no conflict occurred")
	}
	klog.Errorf("Conflict error: %v", err)

	// now forcefully update data with owner2 which will eventually make owner2 the only owner of the `username` key
	secret2.ManagedFields = nil
	secret2.Data = data
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner2, client.ForceOwnership)
	if err != nil {
		panic(err)
	}
	printOwnerInfo(kc, key)

	// ============ Conflicts Example #3 (Don't overwrite value, give up management claim) =============== //
	// now owner2 don't overwrite `password` key and give up ownership of password key
	secret2.ManagedFields = nil
	newData := map[string][]byte{
		"username": []byte("owner2"),
	}
	secret2.Data = newData
	err = kc.Patch(context.TODO(), secret2, client.Apply, owner2)
	if err != nil {
		panic(err)
	}
	printOwnerInfo(kc, key)
}

func printOwnerInfo(kc client.Client, key client.ObjectKey) {
	createdSecret := core.Secret{}
	err := kc.Get(context.TODO(), key, &createdSecret)
	if err != nil {
		panic(err)
	}
	fmt.Println(createdSecret.ManagedFields)
	fmt.Print("Press any key to continue...")
	fmt.Scanln()
}

func cleanSecret(kc client.Client, key client.ObjectKey) {
	// cleanup secret
	secret := core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}
	err := kc.Delete(context.TODO(), &secret)
	if err != nil {
		panic(err)
	}
	klog.Info("secret deleted")
}
