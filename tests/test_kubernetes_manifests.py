import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def read_documents(relative_path):
    path = ROOT / relative_path
    return [doc for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")) if doc]


class KubernetesManifestTests(unittest.TestCase):
    def test_rollout_has_availability_security_probes_and_blue_green_gate(self):
        rollout = read_documents("k8s/base/rollout.yaml")[0]
        self.assertEqual(rollout["kind"], "Rollout")
        self.assertGreaterEqual(rollout["spec"]["replicas"], 3)
        pod = rollout["spec"]["template"]["spec"]
        self.assertTrue(pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(pod["securityContext"]["seccompProfile"]["type"], "RuntimeDefault")
        container = pod["containers"][0]
        self.assertIn("requests", container["resources"])
        self.assertIn("limits", container["resources"])
        self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
        self.assertTrue({"startupProbe", "readinessProbe", "livenessProbe"}.issubset(container))
        strategy = rollout["spec"]["strategy"]["blueGreen"]
        self.assertFalse(strategy["autoPromotionEnabled"])
        self.assertIn("prePromotionAnalysis", strategy)

    def test_canary_overlay_requires_pause_and_analysis(self):
        kustomization = read_documents("k8s/overlays/canary/kustomization.yaml")[0]
        patch = yaml.safe_load(kustomization["patches"][0]["patch"])
        canary = patch[0]["value"]["canary"]
        self.assertEqual(canary["stableService"], "url-shortener-active")
        self.assertEqual(canary["canaryService"], "url-shortener-preview")
        self.assertTrue(any("analysis" in step for step in canary["steps"]))
        self.assertTrue(any(step.get("pause") == {} for step in canary["steps"]))

    def test_hpa_pdb_and_worker_have_resource_and_probe_controls(self):
        autoscalers = read_documents("k8s/base/autoscaling.yaml")
        self.assertEqual(len(autoscalers), 2)
        self.assertEqual(autoscalers[0]["spec"]["scaleTargetRef"]["kind"], "Rollout")
        worker_deployment, worker_pdb = read_documents("k8s/base/analytics-worker.yaml")[1:]
        worker = worker_deployment["spec"]["template"]["spec"]["containers"][0]
        self.assertIn("resources", worker)
        self.assertIn("readinessProbe", worker)
        self.assertEqual(worker_pdb["spec"]["minAvailable"], 2)

    def test_local_overlay_is_explicitly_dev_only_and_has_stateful_dependencies(self):
        kustomization = read_documents("k8s/overlays/local/kustomization.yaml")[0]
        secret_literal = "\n".join(kustomization["secretGenerator"][0]["literals"])
        self.assertIn("dev-only-change-me", secret_literal)
        kinds = {
            item["kind"]
            for name in ("postgres.yaml", "redpanda.yaml", "redis.yaml")
            for item in read_documents(f"k8s/local-dependencies/{name}")
        }
        self.assertIn("StatefulSet", kinds)
        self.assertIn("Deployment", kinds)

    def test_local_redis_uses_non_root_security_context(self):
        redis_deployment = read_documents("k8s/local-dependencies/redis.yaml")[1]
        pod = redis_deployment["spec"]["template"]["spec"]
        self.assertTrue(pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(pod["securityContext"]["runAsUser"], 999)
        self.assertEqual(pod["securityContext"]["seccompProfile"]["type"], "RuntimeDefault")

    def test_rollout_analysis_uses_local_prometheus_service_dns(self):
        analysis = read_documents("k8s/base/analysis.yaml")[0]
        self.assertEqual(
            analysis["spec"]["args"][0]["value"],
            "http://prometheus.url-shortener.svc.cluster.local:9090",
        )
        production = read_documents("k8s/overlays/production/kustomization.yaml")[0]
        analysis_patch = next(
            patch
            for patch in production["patches"]
            if patch["target"].get("kind") == "AnalysisTemplate"
        )
        operations = yaml.safe_load(analysis_patch["patch"])
        self.assertTrue(any(op["path"] == "/spec/args/0/value" for op in operations))

    def test_production_network_controls_and_gitops_contracts_exist(self):
        policy_kustomization = read_documents(
            "k8s/overlays/production-hardening/kustomization.yaml"
        )[0]
        self.assertIn("network-policies.yaml", policy_kustomization["resources"])
        policies = read_documents("k8s/overlays/production-hardening/network-policies.yaml")
        self.assertTrue(any(policy["metadata"]["name"] == "default-deny" for policy in policies))
        self.assertTrue(any(policy["metadata"]["name"] == "api-network" for policy in policies))

        argocd = read_documents("k8s/gitops/argocd-application.yaml")[0]
        flux_source, flux_kustomization = read_documents("k8s/gitops/flux-source.yaml")
        self.assertIn("REPLACE_WITH", argocd["spec"]["source"]["repoURL"])
        self.assertIn("REPLACE_WITH", flux_source["spec"]["url"])
        self.assertFalse(argocd["spec"]["syncPolicy"]["automated"]["prune"])
        self.assertFalse(flux_kustomization["spec"]["prune"])
        self.assertEqual(
            flux_kustomization["spec"]["healthChecks"][0]["kind"], "Rollout"
        )

    def test_ingress_has_tls_and_production_overlay_rewrites_hostname(self):
        ingress = read_documents("k8s/base/ingress.yaml")[0]
        self.assertEqual(ingress["spec"]["tls"][0]["secretName"], "url-shortener-tls")
        production = read_documents("k8s/overlays/production/kustomization.yaml")[0]
        ingress_patch = next(
            patch for patch in production["patches"] if patch["target"].get("kind") == "Ingress"
        )
        operations = yaml.safe_load(ingress_patch["patch"])
        self.assertTrue(any(op["path"] == "/spec/tls/0/hosts/0" for op in operations))


if __name__ == "__main__":
    unittest.main()
