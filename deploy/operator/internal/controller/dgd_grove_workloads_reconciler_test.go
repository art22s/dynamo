/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controller

import (
	"context"
	"errors"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/config/v1alpha1"
	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	commoncontroller "github.com/ai-dynamo/dynamo/deploy/operator/internal/controller_common"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/provideroverride"
	grovev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestGroveWorkloadsReconciler_EvaluatesReadinessOnce(t *testing.T) {
	t.Log("Build one ready Grove component and record readiness reads")
	dgd := betaDGD(t, &nvidiacomv1alpha1.DynamoGraphDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "graph", Namespace: "default"},
		Spec: nvidiacomv1alpha1.DynamoGraphDeploymentSpec{
			BackendFramework: "vllm",
			Services: map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
				"frontend": {
					ComponentType: consts.ComponentTypeFrontend,
					Replicas:      ptr.To(int32(1)),
				},
			},
		},
	})
	podClique := &grovev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "graph-0-frontend",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: grovev1alpha1.PodCliqueSpec{Replicas: 1},
		Status: grovev1alpha1.PodCliqueStatus{
			Replicas:           1,
			ReadyReplicas:      1,
			UpdatedReplicas:    1,
			ScheduledReplicas:  1,
			ObservedGeneration: ptr.To(int64(1)),
		},
	}

	podCliqueReads := 0
	scaleClient := &recordingGroveScaleClient{}
	kubeClient := fake.NewClientBuilder().
		WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
		WithObjects(dgd, podClique).
		WithStatusSubresource(dgd, podClique).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(
				ctx context.Context,
				reader client.WithWatch,
				key client.ObjectKey,
				object client.Object,
				options ...client.GetOption,
			) error {
				if _, ok := object.(*grovev1alpha1.PodClique); ok {
					require.Len(t, scaleClient.updates, 1, "readiness must be observed after scaling")
					podCliqueReads++
				}
				return reader.Get(ctx, key, object, options...)
			},
		}).
		Build()
	reconciler := &DynamoGraphDeploymentReconciler{
		Client:        kubeClient,
		Config:        &configv1alpha1.OperatorConfiguration{},
		Recorder:      events.NewFakeRecorder(10),
		RuntimeConfig: &commoncontroller.RuntimeConfig{},
		ScaleClient:   scaleClient,
		DockerSecretRetriever: &mockDockerSecretRetriever{
			GetSecretsFunc: func(string, string) ([]string, error) {
				return nil, nil
			},
		},
	}

	t.Log("Reconcile the complete Grove workload sequence")
	result, err := reconciler.newGroveProgram().workloads.Reconcile(
		context.Background(),
		dgd,
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, nvidiacomv1beta1.DGDStateSuccessful, result.State)
	assert.Equal(t, 1, podCliqueReads)
}

func TestGroveProviderOverridesUseOrdinaryPCSReconciliation(t *testing.T) {
	t.Log("Build one Grove reconciler with an injectable ordinary update failure")
	dgd := &nvidiacomv1beta1.DynamoGraphDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "graph", Namespace: "default", UID: types.UID("graph-uid")},
	}
	desired := &grovev1alpha1.PodCliqueSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: provideroverride.GroveAPIVersion, Kind: provideroverride.TargetPodCliqueSet},
		ObjectMeta: metav1.ObjectMeta{Name: "graph", Namespace: "default"},
		Spec: grovev1alpha1.PodCliqueSetSpec{
			Template: grovev1alpha1.PodCliqueSetTemplateSpec{},
		},
	}
	rejectUpdate := false
	kubeClient := fake.NewClientBuilder().
		WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, writer client.WithWatch, object client.Object, opts ...client.UpdateOption) error {
				if rejectUpdate {
					return errors.New("provider rejected update")
				}
				return writer.Update(ctx, object, opts...)
			},
		}).
		Build()
	reconciler := &groveWorkloadsReconciler{
		syncer: newDGDResourceSyncer(kubeClient, events.NewFakeRecorder(10)),
	}

	t.Log("Create the PCS after composing an opaque root topology override")
	dgd.Spec.ProviderOverride = rootTopologyOverride(`{"topologyName":"gpu-topology"}`)
	providerObject, err := provideroverride.ComposeGroveOverrides(dgd, desired)
	require.NoError(t, err)
	_, err = reconciler.reconcilePodCliqueSet(context.Background(), dgd, providerObject)
	require.NoError(t, err)
	assertLiveRootTopologyValue(t, kubeClient, "topologyName", "gpu-topology")

	t.Log("Reject an ordinary PCS update and verify the live resource remains unchanged")
	dgd.Spec.ProviderOverride = rootTopologyOverride(`{"topologyName":"changed"}`)
	providerObject, err = provideroverride.ComposeGroveOverrides(dgd, desired)
	require.NoError(t, err)
	rejectUpdate = true
	_, err = reconciler.reconcilePodCliqueSet(context.Background(), dgd, providerObject)
	require.ErrorContains(t, err, "provider rejected update")
	assertLiveRootTopologyValue(t, kubeClient, "topologyName", "gpu-topology")

	t.Log("Remove the override and let the same PCS update path prune its subtree")
	rejectUpdate = false
	dgd.Spec.ProviderOverride = nil
	providerObject, err = provideroverride.ComposeGroveOverrides(dgd, desired)
	require.NoError(t, err)
	_, err = reconciler.reconcilePodCliqueSet(context.Background(), dgd, providerObject)
	require.NoError(t, err)
	live := &unstructured.Unstructured{}
	live.SetAPIVersion(provideroverride.GroveAPIVersion)
	live.SetKind(provideroverride.TargetPodCliqueSet)
	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(desired), live))
	_, found, nestedErr := unstructured.NestedFieldNoCopy(live.Object, "spec", "template", "topologyConstraint")
	require.NoError(t, nestedErr)
	assert.False(t, found)
}

func rootTopologyOverride(topology string) *nvidiacomv1beta1.ProviderOverride {
	return &nvidiacomv1beta1.ProviderOverride{
		APIVersion: provideroverride.GroveAPIVersion,
		Target:     provideroverride.TargetPodCliqueSet,
		Value: apiextensionsv1.JSON{Raw: []byte(
			`{"spec":{"template":{"topologyConstraint":` + topology + `}}}`,
		)},
	}
}

func assertLiveRootTopologyValue(
	t *testing.T,
	kubeClient client.Client,
	field string,
	want interface{},
) {
	t.Helper()
	live := &unstructured.Unstructured{}
	live.SetAPIVersion(provideroverride.GroveAPIVersion)
	live.SetKind(provideroverride.TargetPodCliqueSet)
	require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKey{Name: "graph", Namespace: "default"}, live))
	got, found, err := unstructured.NestedFieldNoCopy(
		live.Object,
		"spec",
		"template",
		"topologyConstraint",
		field,
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, want, got)
}
