import os
import jpype
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-oracle/'
#os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-7-openjdk-amd64/'
#os.environ['NEO4J_PYTHON_JVMARGS'] = '-Xms256M -Xmx1024M'
from neo4j import GraphDatabase, INCOMING, Evaluation, OUTGOING, ANY

c1 = c2 = 0

#Definicao do banco de dados
def inicia_bd(x):
	global db, pesquisador_idx
	db = GraphDatabase('teste')
	with db.transaction:
		pesquisador = db.node()
		if x==0:
			pesquisador_idx = db.node.indexes.create('pesquisadores')
		if x==1:
			pesquisador_idx = db.node.indexes.get('pesquisadores')
	return

#Funcao para criar os pesquisadores e seus atributos
#def criar_pesq(ide, nome, instituicao, ano, pais, titulo):
def criar_pesq(ide, nome):
	global c1
	with db.transaction:
		pesquisador = db.node(id=ide)
		pesquisador['nome'] = nome
		pesquisador_idx['id'][ide] = pesquisador
		c1 += 1
		print('criando pesquisador: %d' % pesquisador['id'])
		print('pesquisador #%d' % c1)
	return pesquisador

#Funcao para receber um id e retornar o pesquisador correspondente	
def get_pesquisador(ide):
	pesquisador = pesquisador_idx['id'][ide].single
	print pesquisador['id']
	return pesquisador
	
#Funcao para criar os relacionamentos e seus atributos
def criar_rel(ORIGEM, DESTINO):
	global c2
	with db.transaction:
		relacionamento = ORIGEM.SENT_TO(DESTINO)
		c2 += 1
		print('criando relacionamento: %d' % c2)
	return

#Leitura dos dados de origem e criacao dos pesquisadores
inicia_bd(0)
arq = open('teste.txt','r')
for p in arq.readlines():
	criar_pesq(int(p.split(';')[0]), (p.split(';')[1]))
arq.close()
db.shutdown()

#Leitura dos dados de origem e criacao dos relacionamentos
inicia_bd(1)
arq = open('teste.txt','r')
for p in arq.readlines():
	aux = len(p.split(';'))
	for e in range(2,aux-1):
		criar_rel(get_pesquisador(int(p.split(';')[0])), get_pesquisador(int(p.split(';')[e])))
arq.close()		
db.shutdown()
print('Foram inseridos %d vertices' %c1)
print('e %d arestas' %c2)


