import io
import os.path
import re
import sys
from collections import defaultdict

import ruamel.yaml  # Use this library over regular yaml (pyyaml) because it round-trips better (long strings, comments)


path_map = {
    'cost-analyzer/charts/prometheus/templates': 'prometheus',
    'cost-analyzer/templates': 'cost-analyzer',
}


image_map = {
    'jimmidyson': 'jimmidyson',
    'quay.io/prometheus': '',
    'gcr.io/kubecost1': 'kubecost1',
    'prom': 'node-exporter',
    'michaelkubecost': 'michaelkubecost'
}


def remap_to_lyft_ecr(orig_image):
    orig_path, image = os.path.split(orig_image)  # gcr.io/kubecost1/cost-model:prod-1.101.3 --> ('gcr.io/kubecost1', 'cost-model:prod-1.101.3')
    lyft_path = image_map.get(orig_path)
    if lyft_path is None:
        print(f'*** ERROR: No matching ECR path found in image_map for {orig_image}')
        print(f'orig_path: {orig_path}')
        sys.exit(-1)
    return os.path.join(lyft_path, image)


def modify_generated_k8s_object(_dict, path=[]):
    """
    Modifies a k8s object to strip out any Helm-related data, and remap image paths for containers to Lyft equivalent
    """
    keys_to_prune = []
    for k, v in _dict.items():        
        if 'helm' in k.lower() or (isinstance(v, str) and 'helm' in v.lower()):
            # Remove this entire entry from the dict if the key or anything in the value contains 'helm'
            keys_to_prune.append(k)
        elif isinstance(v, dict):
            # If the value is a dict, recurse
            modify_generated_k8s_object(v, path + [k])
        elif isinstance(v, list):
            # If the value is a list, inspect each of the elements looking for dicts to recurse on
            index_to_delete = None
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    # HACK: for property-bags with {'name': 'HELM_VALUES', 'value': <foo>}, mark the property-bag for deletion from the list
                    if item.get('name') == 'HELM_VALUES':
                        index_to_delete = i
                        break
                    else:
                        modify_generated_k8s_object(item, path + [k])                    
            if index_to_delete is not None:
                print(f'--8<-- Pruning {".".join(path + [k])}[{index_to_delete}] {v[index_to_delete]["name"]}')
                del v[index_to_delete]
        elif k == 'image' and path[-1] == 'containers':
            lyft_image = remap_to_lyft_ecr(v)
            print(f'<----> Remapping container image {v} --> {lyft_image}')
            _dict[k] = lyft_image

    # Kubecost specifies some invalid resources in the '' apiGroup when defining a ClusterRole that will fail deployment if we keep them
    if _dict.get('kind') == 'ClusterRole':
        try:
            default_api_group_rules = next(rule_group for rule_group in _dict.get('rules', []) if rule_group.get('apiGroups') == [''])
            for bad_resource in ('ingresses', 'deployments'):
                if bad_resource in default_api_group_rules['resources']:
                    print(f'--8<-- Pruning bad resource in empty apiGroup: {bad_resource}')
                    default_api_group_rules['resources'].remove(bad_resource)
        except StopIteration:
            # No default API group found
            pass

    for k in keys_to_prune:
        print(f'--8<-- Pruning "{".".join(path + [k])}: {_dict[k]}"')
        del _dict[k]


def escape_template_vars(s):
    pattern = r'\$\{(\w+)\}'  # Picks out strings like "${some_template_var}"
    exclusion_list = ['is_ksm_v2', 'cluster', 'environment', 'prometheus_memory_request']  # Don't escape the interpolation vars we *are* using (assumes no collisions)
    def _insert_backslash_placeholder(match):
        template_var = match.group(1)
        if template_var not in exclusion_list:  
            return '<BACKSLASH>' + match.group(0)
        else:
            return match.group(0)  # pass through the original string
    
    return re.sub(pattern, _insert_backslash_placeholder, s, flags=re.ASCII)    


def generate_yaml(path_to_manifest: str, primary_or_secondary: str):
    yaml = ruamel.yaml.YAML()
    counter = defaultdict(int)
    with open(path_to_manifest) as helm_fp:        
        for k8s_yaml in helm_fp.read().split('---\n'):
            if not k8s_yaml:
                continue
            source = k8s_yaml.split('\n')[0].split()[-1]  # e.g. cost-analyzer/charts/prometheus/templates/server-serviceaccount.yaml
            src_path, _ = os.path.split(source)
            dst_path = path_map.get(src_path)
            if not dst_path:
                print(f'No matching destination path found in path_map for {source}')
                sys.exit(-1)

            # Various config maps use template variables that look like interpolation vars, which breaks k8sdeploy when we
            # feed it the k8s object files generated for Kubecost.
            # Escape these interpolation var-lookalikes with a backslash marker so we can escape it when we write the file
            # to disk (we can't put in a backslash directly here since it will break the yaml parser).
            escaped_k8s_yaml = escape_template_vars(k8s_yaml)

            # Give a number-prefixed filename for the k8s object, based on:
            # - the deployment the k8s object is for
            # - the order it's encountered in the Helm-generated uber-manifest for that deployment
            k8s_object = yaml.load(io.StringIO(escaped_k8s_yaml))
            if not k8s_object:
                continue
            filename = f"{k8s_object['metadata']['name']}-{k8s_object['kind'].lower()}"

            if k8s_object['kind'] == 'ConfigMap' and k8s_object['metadata'].get('labels', {}).get('grafana_dashboard') == '1':
                print(f'--8<-- Skipping {filename} Grafana ConfigMap')
                continue           

            modify_generated_k8s_object(k8s_object)
            ops_dst_path = f'ops/k8s/kubecost-{primary_or_secondary}/{dst_path}'
            os.makedirs(ops_dst_path, exist_ok=True)
            full_dst_path = f'{ops_dst_path}/{counter[dst_path]:03d}-{filename}.yaml'
            with io.StringIO() as yaml_str:
                yaml.dump(k8s_object, yaml_str)                
                with open(full_dst_path, 'w') as k8s_fp:
                    print(f'Prepping {full_dst_path}...')
                    k8s_fp.write(yaml_str.getvalue().replace('<BACKSLASH>', '\\'))  # Replace the special token with a backslash
                    print(f'==> wrote {full_dst_path}')
            counter[dst_path] += 1


def usage():
    print('Usage: python de-helmify.py <path to k8s manifest>  <"primary" or "secondary">')
    sys.exit(-1)


if __name__ == '__main__':
    if len(sys.argv) < 3 or sys.argv[2] not in ('primary', 'secondary', 'waterfowl'):
        usage()

    generate_yaml(sys.argv[1], sys.argv[2])