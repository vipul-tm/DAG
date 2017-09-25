import memcache;
import telnetlib
MEMCACHE_CONFIG = "10.133.19.165:11211";
memc_cnx= memcache.Client([MEMCACHE_CONFIG], debug=1 );
#memc_cnx.set('nw_nocout_ospf1','')


def get_all_memcached_keys(host='10.133.19.165', port=11211):
    t = telnetlib.Telnet(host, port)
    t.write('stats items STAT items:0:number 0 END\n')
    items = t.read_until('END').split('\r\n')
    keys = set()
    for item in items:
        parts = item.split(':')
        if not len(parts) >= 3:
            continue
        slab = parts[1]
    
        t.write('stats cachedump %s 200000 ITEM views.decorators.cache.cache_header..cc7d9 [6 b; 1256056128 s] END\n'%(slab))
        cachelines = t.read_until('END').split('\r\n')
        for line in cachelines:
            parts = line.split(' ')
            if not len(parts) >= 3:
                continue
            keys.add(parts[1])
    t.close()
    return keys

#keys = get_all_memcached_keys()
#print keys
#print memc_cnx.get('sv_ospf1_slave_1_slot_1_result')
#print len(memc_cnx.get('sv_nocout_ospf5'))
#print len(memc_cnx.get('sv_ospf1_slave_1_slot_1_result'))
#print memc_cnx.get('sv_ospf1_slave_1_slot_1_result')
print "WIMAX INTRF UL /DL"
print memc_cnx.get('%s_wimax_ul_intrf' % '10.172.206.40')
print memc_cnx.get('%s_wimax_dl_intrf' % '10.172.206.40')
print memc_cnx.get('%s_pmp1_sec' % '26310')
print memc_cnx.get('%s_pmp2_sec' % '26310')

print "CAMBIUM JITTER REREG"

print memc_cnx.get('%s_cambium_ul_jitter' % '10.171.28.51')
print memc_cnx.get('%s_cambium_rereg_count' % '10.171.28.51')

print "uas rad5k"
print memc_cnx.get('%s_uas_list' % '10.172.206.40')
print memc_cnx.get('%s_rad5k_ss_ul_mod' % '10.172.206.40')


print memc_cnx.get('%s_rad5kjet_uas_list' % '10.133.26.79')
print memc_cnx.get('%s_rad5kjet_ss_ul_mod' % '10.133.26.79')


    