from airflow.hooks import RedisHook
from airflow.models import Variable
import time

d=[]
dik = {}
all_dic = {}
all_data_dic= {}
redis_hook_2 = RedisHook(redis_conn_id="redis_hook_2")
util_tech = eval(Variable.get("utilization_kpi_technologies"))
attri=eval(Variable.get("utilization_kpi_attributes"))
hotmk=eval(Variable.get("hostmk.dict.site_mapping"))

for a in attri:
	serv = attri.get(a)
	dik[a] = []
	all_data_dic["kpi_util_prev_state_%s"%a] = {}
	for s in serv:
		dik[a].append(s.get('service_name'))

for tech in util_tech:
	sites = hotmk.get(tech)
	for site in sites:
		site=sites.get(site)
		for f in site:
			 d.append((f.get('hostname'),tech))			


print d[0]
for devices in d:
	for sv in dik.get(devices[1]):
		all_dic[devices[0]+"_"+sv] = {'since':time.time(),'state':'ok'}
		all_data_dic["kpi_util_prev_state_%s"%(devices[1])].update(all_dic)



print all_data_dic.keys()  


for keys in all_data_dic:
	redis_hook_2.set(keys,str(all_data_dic.get(keys)))
