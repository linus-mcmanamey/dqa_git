rebuild_dmc:
	@pip install git+https://dev.azure.com/emstas/Program%20Unify/_git/unify_2_1_dm_core

dev: check_kerberos rebuild_dmc
	@python3 main.py

check_kerberos:
	@klist -s || (echo "Kerberos ticket is not valid. Please Enter your Password." && kinit $(USERNAME)@POLICE.TAS.GOV.AU -l 24h)

# test:
# 	@streamlit run app.py --server.port=8002 --server.address=localhost

run: check_kerberos
	#@uv run src/spark_connector.py
	@python main.py

#SynSQlAdmin
