package declarative

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/alpha/patterns/addon/pkg/loaders"
	"sigs.k8s.io/controller-runtime/alpha/patterns/declarative/pkg/kubectlcmd"
	"sigs.k8s.io/controller-runtime/alpha/patterns/declarative/pkg/manifest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/runtime/log"
	//applicationclient "github.com/kubernetes-sigs/application/pkg/client/clientset/versioned"
)

var _ reconcile.Reconciler = &Reconciler{}

type Reconciler struct {
	prototype DeclarativeObject
	client    client.Client
	config    *rest.Config
	kubectl   kubectlClient

	// Client that can manage Application CRD
	//applicationClient *applicationclient.Clientset

	packageName string
	options     reconcilerParams
}

type kubectlClient interface {
	Apply(ctx context.Context, namespace string, manifest string, args ...string) error
}

type DeclarativeObject interface {
	runtime.Object
}

// For mocking
var kubectl = kubectlcmd.New()

func (r *Reconciler) Init(mgr manager.Manager, prototype DeclarativeObject, packageName string, opts ...reconcilerOption) {
	/*if !initialized {
		panic("attempting to initialize declarative.Reconciler without calling addon.Init() setup function")
	}
	*/

	r.prototype = prototype
	r.packageName = packageName
	r.kubectl = kubectl

	r.client = mgr.GetClient()
	r.config = mgr.GetConfig()

	/*
		var err error
			r.applicationClient, err = applicationclient.NewForConfig(mgr.GetConfig())
			if err != nil {
				panic(err)
			}
	*/

	r.applyOptions(opts...)

	/*
		if r.options.groupVersionKind == nil {
			panic("GroupVersionKind is a required option, add operators.WithGroupVersionKind(...) to this method call")
		}
	*/
}

// +rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
func (r *Reconciler) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx := context.TODO()
	log := log.Log

	// Fetch the object
	instance := r.prototype.DeepCopyObject().(DeclarativeObject)
	err := r.client.Get(ctx, request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "error reading object")
		return reconcile.Result{}, err
	}

	return r.reconcileExists(ctx, request.NamespacedName, instance)
}

func (r *Reconciler) reconcileExists(ctx context.Context, name types.NamespacedName, instance DeclarativeObject) (reconcile.Result, error) {
	log := log.Log
	log.WithValues("object", name.String()).Info("reconciling")

	objects, err := r.BuildDeploymentObjects(ctx, name, instance)
	if err != nil {
		log.Error(err, "building deployment objects")
		return reconcile.Result{}, fmt.Errorf("error building deployment objects: %v", err)
	}

	log.WithValues("objects", fmt.Sprintf("%d", len(objects.Items))).Info("built deployment objects")

	/*
		app, err := r.ensureApplication(ctx, name, instance, objects)
		if err != nil {
			log.Error(err, "ensuring application")
			return reconcile.Result{}, err
		}
		defer r.reconcileStatus(ctx, name, instance, app, objects)

		err = r.injectOwnerRef(ctx, instance, objects)
		if err != nil {
			return reconcile.Result{}, err
		}
	*/

	m, err := objects.JSONManifest()
	if err != nil {
		log.Error(err, "creating final manifest")
	}

	extraArgs := []string{"--force"}
	/*
		if r.options.prune {
				selectorArg := ""
				for k, v := range r.labels(name) {
					if selectorArg != "" {
						selectorArg += ","
					}
					selectorArg += fmt.Sprintf("%s=%s", k, v)
				}

				extraArgs = append(extraArgs, "--prune", "--selector", selectorArg)
			}
	*/

	if err := r.kubectl.Apply(ctx, name.Namespace, m, extraArgs...); err != nil {
		log.Error(err, "applying manifest")

		return reconcile.Result{}, fmt.Errorf("error applying manifest: %v", err)
	}

	/*
		if err := r.ensureWatches(ctx, name, objects); err != nil {
			log.Error(err, "watching deployed object types")
			panic(fmt.Errorf("error watching deployed object types: %v", err))
		}
	*/

	return reconcile.Result{}, nil
}

// BuildDeploymentObjects performs all manifest operations to build a final set of objects for deployment
func (r *Reconciler) BuildDeploymentObjects(ctx context.Context, name types.NamespacedName, instance DeclarativeObject) (*manifest.Objects, error) {
	log := log.Log

	// 1. Load the manifest
	manifestStr, err := r.loadRawManifest(ctx, instance)
	if err != nil {
		log.Error(err, "error loading raw manifest")
		return nil, err
	}

	// 2. Perform raw string operations
	for _, t := range r.options.rawManifestOperations {
		transformed, err := t(ctx, instance, manifestStr)
		if err != nil {
			log.Error(err, "error performing raw manifest operations")
			return nil, err
		}
		manifestStr = transformed
	}

	// 3. Parse manifest into objects
	objects, err := manifest.ParseObjects(ctx, manifestStr)
	if err != nil {
		log.Error(err, "error parsing manifest")
		return nil, err
	}

	// 4. Perform object transformations

	// Always add a fixed set of labels for grouping with Application
	//transforms := append(r.options.objectTransformations, AddLabels(r.labels(name)))
	transforms := r.options.objectTransformations
	for _, t := range transforms {
		err := t(ctx, instance, objects)
		if err != nil {
			return nil, err
		}
	}

	// 5. Sort objects to work around dependent objects in the same manifest (eg: service-account, deployment)
	objects.Sort(DefaultObjectOrder(ctx))

	return objects, nil
}

// loadRawManifest loads the raw manifest YAML from the repository
func (r *Reconciler) loadRawManifest(ctx context.Context, o DeclarativeObject) (string, error) {
	s, err := r.options.manifestController.ResolveManifest(ctx, o)
	if err != nil {
		return "", err
	}

	return s, nil
}

func (r *Reconciler) applyOptions(opts ...reconcilerOption) {
	params := reconcilerParams{}

	/*
		rc := getRuntimeConfig()
		if rc.ManifestSource == Filesystem {
			params.manifestController = manifest.NewController()
		} // TODO support https / bundles?
		if rc.PrivateImageRepository != nil {
			params.objectTransformations = append(params.objectTransformations, withImageRegistryTransform(rc.PrivateImageRepository.Repository, rc.PrivateImageRepository.ImagePullSecret))
		}
	*/

	for _, opt := range opts {
		params = opt(params)
	}

	// Default to filesystem manifest controller if not set
	// TODO: Default to https?
	if params.manifestController == nil {
		params.manifestController = loaders.NewManifestLoader()
	}

	r.options = params
}
