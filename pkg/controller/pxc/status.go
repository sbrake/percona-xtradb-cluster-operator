package pxc

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/percona/percona-xtradb-cluster-operator/pkg/apis/pxc/v1"
	"github.com/percona/percona-xtradb-cluster-operator/pkg/pxc/app/statefulset"
	"github.com/pkg/errors"
)

const maxStatusesQuantity = 20

func (r *ReconcilePerconaXtraDBCluster) updateStatus(cr *api.PerconaXtraDBCluster, reconcileErr error) (err error) {
	clusterCondition := api.ClusterCondition{
		Status:             api.ConditionTrue,
		Type:               api.ClusterInit,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}

	if reconcileErr != nil {
		if cr.Status.Status != api.AppStateError {
			clusterCondition = api.ClusterCondition{
				Status:             api.ConditionTrue,
				Type:               api.ClusterError,
				Message:            reconcileErr.Error(),
				Reason:             "ErrorReconcile",
				LastTransitionTime: metav1.NewTime(time.Now()),
			}
			cr.Status.Conditions = append(cr.Status.Conditions, clusterCondition)

			cr.Status.Messages = append(cr.Status.Messages, "Error: "+reconcileErr.Error())
			cr.Status.Status = api.AppStateError
		}

		return r.writeStatus(cr)
	}

	cr.Status.Messages = cr.Status.Messages[:0]

	pxc := statefulset.NewNode(cr)
	pxcStatus, condition, err := r.appStatus(pxc, cr.Namespace, cr.Spec.PXC.PodSpec, cr.Status.PXC, cr.CompareVersionWith("1.7.0") >= 0)
	if err != nil {
		return fmt.Errorf("get pxc status: %v", err)
	}

	if pxcStatus.Status != cr.Status.PXC.Status {
		clusterCondition = condition
	}
	cr.Status.PXC = pxcStatus

	host, err := r.appHost(pxc, cr.Namespace, cr.Spec.PXC.PodSpec)
	if err != nil {
		return errors.Wrap(err, "get pxc host")
	}
	cr.Status.Host = host

	if cr.Status.PXC.Message != "" {
		cr.Status.Messages = append(cr.Status.Messages, "PXC: "+cr.Status.PXC.Message)
	}

	inProgress := false

	cr.Status.HAProxy = api.AppStatus{}
	if cr.HAProxyEnabled() {
		haproxy := statefulset.NewHAProxy(cr)

		haProxyStatus, condition, err := r.appStatus(haproxy, cr.Namespace, cr.Spec.HAProxy, cr.Status.HAProxy, cr.CompareVersionWith("1.7.0") >= 0)
		if err != nil {
			return errors.Wrap(err, "get haproxy status")
		}

		if haProxyStatus.Status != cr.Status.HAProxy.Status {
			clusterCondition = condition
		}

		cr.Status.HAProxy = haProxyStatus

		host, err := r.appHost(haproxy, cr.Namespace, cr.Spec.HAProxy)
		if err != nil {
			return errors.Wrap(err, "get haproxy host")
		}
		cr.Status.Host = host

		if cr.Status.HAProxy.Message != "" {
			cr.Status.Messages = append(cr.Status.Messages, "HAProxy: "+cr.Status.HAProxy.Message)
		}
		inProgress, err = r.upgradeInProgress(cr, haproxy.Name())
		if err != nil {
			return errors.Wrap(err, "check haproxy upgrade progress")
		}
	}

	cr.Status.ProxySQL = api.AppStatus{}
	if cr.ProxySQLEnabled() {
		proxy := statefulset.NewProxy(cr)

		proxyStatus, condition, err := r.appStatus(proxy, cr.Namespace, cr.Spec.ProxySQL, cr.Status.ProxySQL, cr.CompareVersionWith("1.7.0") >= 0)
		if err != nil {
			return errors.Wrap(err, "get proxysql status")
		}

		if proxyStatus.Status != cr.Status.ProxySQL.Status {
			clusterCondition = condition
		}

		cr.Status.ProxySQL = proxyStatus

		host, err := r.appHost(proxy, cr.Namespace, cr.Spec.ProxySQL)
		if err != nil {
			return errors.Wrap(err, "get proxysql host")
		}
		cr.Status.Host = host

		if cr.Status.ProxySQL.Message != "" {
			cr.Status.Messages = append(cr.Status.Messages, "ProxySQL: "+cr.Status.ProxySQL.Message)
		}
		inProgress, err = r.upgradeInProgress(cr, proxy.Name())
		if err != nil {
			return errors.Wrap(err, "check proxysql upgrade progress")
		}
	}

	if !inProgress {
		inProgress, err = r.upgradeInProgress(cr, pxc.Name())
		if err != nil {
			return fmt.Errorf("check pxc upgrade progress: %v", err)
		}
	}

	switch {
	case cr.Status.PXC.Status == api.AppStateError ||
		cr.Status.ProxySQL.Status == api.AppStateError ||
		cr.Status.HAProxy.Status == api.AppStateError:
		clusterCondition = api.ClusterCondition{
			Status:             api.ConditionTrue,
			Type:               api.ClusterError,
			LastTransitionTime: metav1.NewTime(time.Now()),
		}
		cr.Status.Status = api.AppStateError
	case cr.Status.PXC.Status == api.AppStateInit ||
		(cr.ProxySQLEnabled() && cr.Status.ProxySQL.Status == api.AppStateInit) ||
		(cr.HAProxyEnabled() && cr.Status.HAProxy.Status == api.AppStateInit):
		clusterCondition = api.ClusterCondition{
			Status:             api.ConditionTrue,
			Type:               api.ClusterInit,
			LastTransitionTime: metav1.NewTime(time.Now()),
		}
		cr.Status.Status = api.AppStateInit
	case (cr.Status.PXC.Status == cr.Status.ProxySQL.Status && cr.ProxySQLEnabled()) ||
		(cr.Status.PXC.Status == cr.Status.HAProxy.Status && cr.HAProxyEnabled()):
		if cr.Status.PXC.Status == api.AppStateReady {
			clusterCondition = api.ClusterCondition{
				Status:             api.ConditionTrue,
				Type:               api.ClusterReady,
				LastTransitionTime: metav1.NewTime(time.Now()),
			}
		}
		cr.Status.Status = cr.Status.PXC.Status
	case !cr.ProxySQLEnabled() && !cr.HAProxyEnabled() && cr.Status.PXC.Status == api.AppStateReady:
		clusterCondition = api.ClusterCondition{
			Status:             api.ConditionTrue,
			Type:               api.ClusterReady,
			LastTransitionTime: metav1.NewTime(time.Now()),
		}
		cr.Status.Status = cr.Status.PXC.Status
	default:
		cr.Status.Status = api.AppStateUnknown
	}

	if len(cr.Status.Conditions) == 0 {
		cr.Status.Conditions = append(cr.Status.Conditions, clusterCondition)
	} else {
		lastClusterCondition := cr.Status.Conditions[len(cr.Status.Conditions)-1]

		switch {
		case lastClusterCondition.Type != clusterCondition.Type:
			cr.Status.Conditions = append(cr.Status.Conditions, clusterCondition)
		default:
			cr.Status.Conditions[len(cr.Status.Conditions)-1] = lastClusterCondition
		}
	}

	if len(cr.Status.Conditions) > maxStatusesQuantity {
		cr.Status.Conditions = cr.Status.Conditions[len(cr.Status.Conditions)-maxStatusesQuantity:]
	}

	if inProgress {
		cr.Status.Status = api.AppStateInit
	}
	cr.Status.ObservedGeneration = cr.ObjectMeta.Generation
	return r.writeStatus(cr)
}

func (r *ReconcilePerconaXtraDBCluster) writeStatus(cr *api.PerconaXtraDBCluster) error {
	err := r.client.Status().Update(context.TODO(), cr)
	if err != nil {
		// may be it's k8s v1.10 and erlier (e.g. oc3.9) that doesn't support status updates
		// so try to update whole CR
		err := r.client.Update(context.TODO(), cr)
		if err != nil {
			return fmt.Errorf("send update: %v", err)
		}
	}

	return nil
}

func (r *ReconcilePerconaXtraDBCluster) upgradeInProgress(cr *api.PerconaXtraDBCluster, appName string) (bool, error) {
	sfsObj := &appsv1.StatefulSet{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: cr.Name + "-" + appName, Namespace: cr.Namespace}, sfsObj)
	if err != nil {
		return false, err
	}
	return sfsObj.Status.Replicas > sfsObj.Status.UpdatedReplicas, nil
}

func (r *ReconcilePerconaXtraDBCluster) appStatus(app api.StatefulApp, namespace string, podSpec *api.PodSpec, crStatus api.AppStatus, cr170OrGreater bool) (api.AppStatus, api.ClusterCondition, error) {
	list := corev1.PodList{}
	err := r.client.List(context.TODO(),
		&list,
		&client.ListOptions{
			Namespace:     namespace,
			LabelSelector: labels.SelectorFromSet(app.Labels()),
		},
	)
	if err != nil {
		return api.AppStatus{}, api.ClusterCondition{}, errors.Wrap(err, "get pod list")
	}
	sfs := app.StatefulSet()
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: sfs.Name, Namespace: sfs.Namespace}, sfs)
	if err != nil {
		return api.AppStatus{}, api.ClusterCondition{}, errors.Wrap(err, "get statefulset")
	}

	status := api.AppStatus{
		Size:              podSpec.Size,
		Status:            api.AppStateInit,
		LabelSelectorPath: labels.SelectorFromSet(app.Labels()).String(),
	}

	for _, pod := range list.Items {
		for _, cond := range pod.Status.Conditions {
			switch cond.Type {
			case corev1.ContainersReady:
				if cond.Status == corev1.ConditionTrue {
					if !isPXC(app) {
						status.Ready++
					} else {
						isPodReady := true
						if cr170OrGreater {
							isPodWaitingForRecovery, _, err := r.isPodWaitingForRecovery(namespace, pod.Name)
							if err != nil {
								return api.AppStatus{}, api.ClusterCondition{}, errors.Wrapf(err, "parse %s pod logs", pod.Name)
							}
							isPodReady = !isPodWaitingForRecovery && pod.ObjectMeta.Labels["controller-revision-hash"] == sfs.Status.UpdateRevision
						}

						if isPodReady {
							status.Ready++
						}
					}
				} else if cond.Status == corev1.ConditionFalse {
					for _, cntr := range pod.Status.ContainerStatuses {
						if cntr.State.Waiting != nil && cntr.State.Waiting.Message != "" {
							status.Message += cntr.Name + ": " + cntr.State.Waiting.Message + "; "
						}
					}
				}
			case corev1.PodScheduled:
				if cond.Reason == corev1.PodReasonUnschedulable &&
					cond.LastTransitionTime.Time.Before(time.Now().Add(-1*time.Minute)) {
					status.Status = api.AppStateError
					status.Message = cond.Message
				}
			}
		}
	}

	if status.Size == status.Ready {
		status.Status = api.AppStateReady
	}

	var condition api.ClusterCondition

	status.Version = crStatus.Version
	status.Image = crStatus.Image

	switch status.Status {
	case api.AppStateReady:
		condition = api.ClusterCondition{
			Status:             api.ConditionTrue,
			Type:               api.ClusterReady,
			Reason:             app.Name() + "Ready",
			LastTransitionTime: metav1.NewTime(time.Now()),
		}
	case api.AppStateError:
		condition = api.ClusterCondition{
			Status:             api.ConditionTrue,
			Message:            app.Name() + status.Message,
			Reason:             app.Name() + "Error",
			Type:               api.ClusterError,
			LastTransitionTime: metav1.NewTime(time.Now()),
		}
	}

	return status, condition, nil
}

func (r *ReconcilePerconaXtraDBCluster) appHost(app api.StatefulApp, namespace string, podSpec *api.PodSpec) (string, error) {
	if podSpec.ServiceType != corev1.ServiceTypeLoadBalancer {
		return app.Service() + "." + namespace, nil
	}

	svc := &corev1.Service{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: app.Service()}, svc)
	if err != nil {
		return "", errors.Wrapf(err, "get %s service", app.Name())
	}

	var host string

	for _, i := range svc.Status.LoadBalancer.Ingress {
		host = i.IP
		if len(i.Hostname) > 0 {
			host = i.Hostname
		}
	}

	return host, nil
}
