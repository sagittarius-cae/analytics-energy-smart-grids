
## Notes
- This is a solid ERD — 13 entities, clear cardinalities. Before generating, let me size this sensibly: 1 CPU / ~3.7GB RAM in this sandbox, and 15M+ rows means I need vectorized generation (numpy/pandas), not row-by-row loops, or it'll never finish. Here's the volume plan, sized so the hierarchy makes sense down to 15M+ meters:

Entity	Rows	Basis
utility_provider	5	
power_plant	25	~5/provider
renewable_source	40	
energy_storage	20	
scada_dms	10	~2/provider
data_mgmt_system	5	1/provider
substation	500	
power_transformer	1,000	~2/substation
distribution_network	2,000	~4/substation
distribution_transformer	150,000	~75/network
ami_head_end	200	
consumer	13,500,000	some consumers own >1 meter
smart_meter	15,250,000	~100/distribution_transformer