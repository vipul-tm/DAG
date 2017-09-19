# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin
import copy
from flask import Blueprint
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink
from operators.mysql_loader_operator  import MySqlLoaderOperator
from operators.telrad_extractor_operator import TelradExtractor
from operators.nagios_extractor_operator import Nagiosxtractor
from operators.evaluate_transformer_operator import EvaluateTransformer
from operators.list2dict_transformer_operator import List2DictTransformer
from operators.api_extractor_operator import ApiExtractor
# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import  BaseOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.models import XCom
from airflow.models import Variable
from hooks.redis_loader_hook import RedisHook
from hooks.memcache_loader_hook import MemcacheHook
import inspect
import MySQLdb
import json
import MySQLdb.cursors
# Creating a flask admin BaseView
OPERATORS = [MySqlLoaderOperator,TelradExtractor,Nagiosxtractor,EvaluateTransformer,List2DictTransformer,ApiExtractor]
HOOKS = [RedisHook,MemcacheHook]
EXECUTOR = []
MACRO = []
ALL = []
ALL.extend(OPERATORS)
ALL.extend(HOOKS)
ALL.extend(EXECUTOR)
ALL.extend(MACRO)
############################################################################################################################################

class TacView(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.htm
        attributes = []
        data_table = []
        operator_data={}

        for classes in ALL:
            operator_data['name']=str(classes.__name__)
            operator_data['type']=str(inspect.getfile(classes).split("_")[-1].split(".")[0])
            operator_data['module']=str(inspect.getfile(classes).split("_")[-2])
            try:
                operator_data['args']=str(classes.arguments) if classes.arguments else "NA"
            except Exception:
                operator_data['args']= "NOT FOUND"
            
            operator_data['desc']=str(inspect.getdoc(classes))
            operator_data['loc']=str(inspect.getfile(classes))          
            data_table.append(copy.deepcopy(operator_data))       
        
        #data_table = json.dumps(data_table)
        return self.render("tac_plugin/tac.html",attributes=attributes,data_table=data_table)

class intro(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.htm
        attributes = []
        data_table = []
        operator_data={}
        intro_rules= eval(Variable.get("introrules"))
        for data in intro_rules:
            operator_data['rule']=str(data.get('rule'))
            operator_data['syntax']=str(data.get('syntax'))
            operator_data['example']=str(data.get('example'))
            operator_data['mandatory']=str(data.get('mandatory'))
            operator_data['desc']=str(data.get('desc'))          
            data_table.append(copy.deepcopy(operator_data))       
        
        #data_table = json.dumps(data_table)
        return self.render("tac_plugin/intro.html",attributes=attributes,data_table=data_table)


class coverage_analytics(BaseView):
    """docstring for coverage_analytics"""

    def get_data_to_be_processed(self,sql):
        db = MySQLdb.connect(host="10.133.12.163",    
                     user="root",        
                     passwd="root",  
                     db="nocout_24_09_14",
                     port = 3200)       

        # you must create a Cursor object. It will let
        #  you execute all the queries you need
        #cur = db.cursor()
        cursor_dict = db.cursor(MySQLdb.cursors.DictCursor) 


        # Use all the SQL you like
       
        #cur.execute("select * from inventory_basestations")
        cursor_dict.execute(sql)
        # print all the first cell of all the rows
        #for row in cur.fetchall():
        #   print row

        all_data = []
        for x in cursor_dict.fetchall():
            all_data.append(x.copy())

        db.close()
        return all_data

    @expose('/')
    def test(self):
        attributes = []
        processed_data = self.get_data_to_be_processed("select * from inventory_coverageanalyticsprocessedinfo;")
        inp_data_ss =  self.get_data_to_be_processed("select * from inventory_coverageanalyticssubstation;")
        inp_data_bs = self.get_data_to_be_processed("select * from inventory_coverageanalyticsbasestation;")
        return self.render("tac_plugin/coverage.html",attributes=attributes,processed_data=processed_data,input_ss = inp_data_ss , input_bs = inp_data_bs)


    







v = TacView(category="TAC Plugin", name="Operators")
introView = intro(category="TAC Plugin", name="Intro")
coverage = coverage_analytics(category="CA", name="Coverage_analytics")
# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "tac_plugin", __name__,
    template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/tac')


VIEWS = [v,introView,coverage]
BLUEPRINT = [bp]

# Defining the plugin class
class AirflowTacPlugin(AirflowPlugin):
    name = "tac_plugin"
    operators = OPERATORS
    hooks = HOOKS
    executors = EXECUTOR
    macros = MACRO
    admin_views = VIEWS
    flask_blueprints = BLUEPRINT
    #menu_links = [ml,ml2]