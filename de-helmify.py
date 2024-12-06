import io
import os.path
import sys
from collections import defaultdict
import ruamel.yaml

path_map = {
    'flinkoperator': 'flinkoperator'
}

image_map = {
    'docker.io/lyft': 'lyft'
}

def remap_to_lyft_ecr(orig_image):
    orig_path, image = os.path.split(orig_image)
    lyft_path = image_map.get(orig_path)
    if lyft_path is None:
        print(f'*** ERROR: No matching ECR path found in image_map for {orig_image}')
        print(f'orig_path: {orig_path}')
        sys.exit(-1)
    return os.path.join(lyft_path, image)

def modify_generated_k8s_object(_dict, path=[]):
    if not isinstance(_dict, dict):
        return
        
    for k, v in _dict.items():        
        if isinstance(v, dict):
            modify_generated_k8s_object(v, path + [k])
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    modify_generated_k8s_object(item, path + [k])
        elif k == 'image' and path[-1] == 'containers':
            lyft_image = remap_to_lyft_ecr(v)
            print(f'<----> Remapping container image {v} --> {lyft_image}')
            _dict[k] = lyft_image

def generate_yaml(path_to_manifest: str):
    yaml = ruamel.yaml.YAML()
    counter = defaultdict(int)
    try:
        with open(path_to_manifest) as manifest_fp:        
            content = manifest_fp.read()
            for k8s_yaml in content.split('---\n'):
                if not k8s_yaml.strip():
                    continue

                try:
                    k8s_object = yaml.load(io.StringIO(k8s_yaml))
                    if not k8s_object or not isinstance(k8s_object, dict):
                        print(f"Skipping invalid YAML object")
                        continue
                        
                    if 'metadata' not in k8s_object or 'kind' not in k8s_object:
                        print(f"Skipping non-Kubernetes YAML object")
                        continue

                    filename = f"{k8s_object['metadata']['name']}-{k8s_object['kind'].lower()}"
                    modify_generated_k8s_object(k8s_object)
                    
                    ops_dst_path = 'ops/k8s/flink/flinkoperator'
                    os.makedirs(ops_dst_path, exist_ok=True)
                    full_dst_path = f'{ops_dst_path}/{counter["flinkoperator"]:03d}-{filename}.yaml'
                    
                    with open(full_dst_path, 'w') as k8s_fp:
                        print(f'Writing {full_dst_path}...')
                        yaml.dump(k8s_object, k8s_fp)
                        print(f'==> wrote {full_dst_path}')
                    counter["flinkoperator"] += 1
                except Exception as e:
                    print(f"Error processing YAML document: {e}")

    except Exception as e:
        print(f"Error reading manifest file: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python de-helmify.py <path to k8s manifest>')
        sys.exit(1)

    generate_yaml(sys.argv[1])