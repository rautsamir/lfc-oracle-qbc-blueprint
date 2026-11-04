# Network patterns for QBC on serverless (Azure)

QBC runs on serverless compute, outside the customer VNet. It reaches a private Oracle only through a private endpoint from the workspace's Network Connectivity Configuration (NCC).

| Pattern | Source location | Serverless path |
|---|---|---|
| A | Azure-hosted DB in a customer VNet | NCC private endpoint to the Azure resource (or a Private Link Service) |
| B | On-prem DB over ExpressRoute | NCC private endpoint → Private Link Service + Standard Load Balancer → proxy/NVA in the VNet → ExpressRoute → Oracle |
| C | Publicly reachable DB | NCC egress allowlist |

## Pattern B checklist

- [ ] Private Link Service on a Standard Load Balancer in the hub/spoke VNet
- [ ] Proxy or NVA in the backend pool (a PLS can't target on-prem IPs directly)
- [ ] NCC private endpoint rule to the PLS, approved on the customer side, with the domain name set to the Oracle FQDN
- [ ] Only the listener port allowed (for example 1521)
- [ ] Serverless egress policy: deny by default, logs to the SIEM
- [ ] Storage: destination ADLS reachable from serverless (NCC private endpoint for DFS)

## The idle-timeout trap

Private Link Service has a fixed idle timeout of about 5 minutes. Azure Firewall and NAT drop idle flows at about 4 minutes. A long `MIN`/`MAX`, `COUNT(*)`, or slow fetch goes quiet longer than that and the flow is dropped (`ORA-17008`, `ORA-17002`).

- Fix on the connector side: TCP keepalive shorter than the shortest idle timeout on the path.
- Raising a load balancer's idle timeout helps only where it's configurable; the PLS layer is not.
- Index cursor and partition columns so metadata queries finish in seconds.

## Setup vs. runtime

The ingestion wizard lists tables over serverless. Even if runtime ingestion works, setup fails without the serverless path (`QUERY_BASED_CONNECTOR_SOURCE_API_ERROR` in `listNamespaces`). Build the private endpoint first.
