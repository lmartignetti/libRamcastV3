import xml.etree.ElementTree as ET
import json

INPUT = "cl_manifest.xml"
OUTPUT = "config.json"

DEAD_NODES = []  # node ids

namespaces = {
    "": "http://www.geni.net/resources/rspec/3",
    "emulab": "http://www.protogeni.net/resources/rspec/ext/emulab/1",
}

default_namespace="{http://www.geni.net/resources/rspec/3}"

tree = ET.parse(INPUT)
root = tree.getroot()
nodes = root.findall(f"{default_namespace}node")

DEAD_NODES.sort(reverse=True)
for dead_node in DEAD_NODES:
    nodes.pop(dead_node)

data = {"local": None, "remotes": []}

if nodes:
    local_node = nodes[0]
    local_services = local_node.find(f"{default_namespace}services")
    if local_services is not None:
        local_login = local_services.find(f"{default_namespace}login")
        if local_login is not None:
            local_hostname = local_login.get("hostname")
            
            interface = local_node.find(f"{default_namespace}interface")
            if interface is not None:
                ip = interface.find(f"{default_namespace}ip")
                if ip is not None:
                    local_ip = ip.get("address")
                    data["local"] = {"id": 0, "hostname": local_hostname, "ip": local_ip}

for i, node in enumerate(nodes[1:], start=1):
    services = node.find(f"{default_namespace}services")
    if services is not None:
        login = services.find(f"{default_namespace}login")
        if login is not None:
            hostname = login.get("hostname")
            iinterface = node.find(f"{default_namespace}interface")
            if interface is not None:
                ip = interface.find(f"{default_namespace}ip")
                if ip is not None:
                    remote_ip = ip.get("address")
                    data["remotes"].append({"id": i, "hostname": hostname, "ip": remote_ip})

with open(OUTPUT, "w") as file:
    json.dump(data, file, indent=4)

print(f"Generated JSON configuration file saved as {OUTPUT}")
