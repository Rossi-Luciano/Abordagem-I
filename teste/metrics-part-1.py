import os
#import numpy
import time
from time import clock
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-7-oracle/'
#os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-7-openjdk-amd64/'

from neo4j import GraphDatabase, INCOMING, Evaluation, OUTGOING, ANY
from array import array

#acessar o banco de dados
db = GraphDatabase('mathematics')

#identificar as chaves
with db.transaction:
	pesquisador_idx = db.node.indexes.get('pesquisadores')

#funcao para identificar um vertice dado um numero
def get_pesquisador(ide):
	return pesquisador_idx['id'][ide].single

#funcao para contar o numero de vertices em cada ordem ascendente
def Fecundidade(A):
	fecundidade = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', OUTGOING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path) == 1:
				fecundidade = fecundidade + 1
			pass
		A[('fecundidade+')] = fecundidade

	fecundidade = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', INCOMING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path) == 1:
				fecundidade = fecundidade + 1
			pass
		A[('fecundidade-')] = fecundidade
		
	return

def Fertilidade(A):
	fertilidade = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', OUTGOING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path) == 1 and path.end[('fecundidade+')] > 0:
				fertilidade = fertilidade + 1
			pass
		A[('fertilidade+')] = fertilidade

	fertilidade = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', INCOMING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path) == 1 and path.end[('fecundidade-')] > 0:
				fertilidade = fertilidade + 1
			pass
		A[('fertilidade-')] = fertilidade
		
	return

def Descendencia(A):
	descendencia = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', OUTGOING)\
			.breadthFirst()\
			.traverse(A)
		for node in traverser.nodes:
			descendencia = descendencia + 1
			pass
		A[('descendencia+')] = descendencia - 1

	descendencia = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', INCOMING)\
			.breadthFirst()\
			.traverse(A)
		for node in traverser.nodes:
			descendencia = descendencia + 1
			pass
		A[('descendencia-')] = descendencia - 1

	return 

def Descendencia2(A):
	vetor = []
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', OUTGOING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path)==2:
				vetor.append(path.end['id'])
			pass
		if len(vetor) != 0:
			A['decendencia2+'] = vetor
		else:
			A['decendencia2+'] = 0
		
	vetor = []
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', INCOMING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path)==2:
				vetor.append(path.end['id'])
			pass
		if len(vetor) != 0:
			A['decendencia2-'] = vetor
		else:
			A['decendencia2-'] = 0
	return

def Primo(A):
	conjunto=set([])
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', INCOMING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path)==2:
				for r in path.end['decendencia2+']:
					conjunto.add(r)
					
			pass
		if len(conjunto) == 0:
			A['primo+'] = 0
		else:
			A['primo+'] = len(conjunto)-1
			
	conjunto=set([])
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', OUTGOING)\
			.breadthFirst()\
			.traverse(A)
		for path in traverser:
			if len(path)==2:
				for r in path.end['decendencia2-']:
					conjunto.add(r)
					
			pass
		if len(conjunto) == 0:
			A['primo-'] = 0
		else:
			A['primo-'] = len(conjunto)-1	
	return

def Geracoes(A):
	D = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', OUTGOING)\
			.depthFirst()\
			.traverse(A)
		for path in traverser:
			if D < len(path):
				D = len(path)
			pass
		A['geracoes+'] = D

	D = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', INCOMING)\
			.depthFirst()\
			.traverse(A)
		for path in traverser:
			if D < len(path):
				D = len(path)
			pass
		A['geracoes-'] = D
	return

def Orientacoes(A):
	orientacoes = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', OUTGOING)\
			.breadthFirst()\
			.traverse(A)
		for node in traverser.nodes:
			orientacoes = orientacoes + node['fecundidade+']
			pass
		A[('orientacoes+')] = orientacoes
	
	orientacoes = 0
	with db.transaction:
		traverser = db.traversal()\
			.relationships('SENT_TO', INCOMING)\
			.breadthFirst()\
			.traverse(A)
		for node in traverser.nodes:
			orientacoes = orientacoes + node['fecundidade-']
			pass
		A[('orientacoes-')] = orientacoes

	return 


#********************Programa Principal******************************************

#Leitura do arquivo de dados do grafo
arq = open('mathematics.txt','r')
matematicos = []
print("Lendo registros...")
for matematico in arq.readlines():
	matematicos.append(int(matematico.split(';')[0]))
arq.close()

#Contagem do numero de vertices encontrados
floresta = len(matematicos)
print("Total de vertices = %d" % floresta)

cont = 0
for p in matematicos:
	cont +=1
	vertice=get_pesquisador(int(p))
	Fecundidade(vertice)
	print('Fecundidade - %d - %s - %.3f%%' % (vertice['id'],vertice['nome'],float(cont)/float(floresta)*100.00))
cont = 0
for p in matematicos:
	cont +=1
	vertice=get_pesquisador(int(p))
	Fertilidade(vertice)
	print('Fertilidade - %d - %s - %.3f%%' % (vertice['id'],vertice['nome'],float(cont)/float(floresta)*100.00))
cont = 0	
for p in matematicos:
	cont +=1
	vertice=get_pesquisador(int(p))
	Descendencia(vertice)
	print('Descendencia - %d - %s - %.3f%%' % (vertice['id'],vertice['nome'],float(cont)/float(floresta)*100.00))
cont = 0
for p in matematicos:
	cont +=1
	vertice=get_pesquisador(int(p))
	Descendencia2(vertice)
	print('Descendencia2 - %d - %s - %.3f%%' % (vertice['id'],vertice['nome'],float(cont)/float(floresta)*100.00))
cont = 0
for p in matematicos:
	cont +=1
	vertice=get_pesquisador(int(p))
	Primo(vertice)
	print('Primo - %d - %s - %.3f%%' % (vertice['id'],vertice['nome'],float(cont)/float(floresta)*100.00))
cont = 0
for p in matematicos:
	cont +=1
	vertice=get_pesquisador(int(p))
	Geracoes(vertice)
	print('Geracoes - %d - %s - %.3f%%' % (vertice['id'],vertice['nome'],float(cont)/float(floresta)*100.00))
cont = 0
for p in matematicos:
	cont +=1
	vertice=get_pesquisador(int(p))
	Orientacoes(vertice)
	print('Orientacoes - %d - %s - %.3f%%' % (vertice['id'],vertice['nome'],float(cont)/float(floresta)*100.00))


print("Gravando arquivos...")
ascendente = open('metricas.txt','w')
for p in matematicos:
	verticeInteresse = get_pesquisador(int(p))
	ascendente.write('%d;' % p)
	ascendente.write('%d;' % verticeInteresse[('fecundidade+')])
	ascendente.write('%d;' % verticeInteresse[('fertilidade+')])
	ascendente.write('%d;' % verticeInteresse[('descendencia+')])
	ascendente.write('%d;' % verticeInteresse[('primo+')])
	ascendente.write('%d;' % verticeInteresse[('geracoes+')])
	ascendente.write('%d;' % verticeInteresse[('orientacoes+')])
	ascendente.write('%d;' % verticeInteresse[('fecundidade-')])
	ascendente.write('%d;' % verticeInteresse[('fertilidade-')])
	ascendente.write('%d;' % verticeInteresse[('descendencia-')])
	ascendente.write('%d;' % verticeInteresse[('primo-')])
	ascendente.write('%d;' % verticeInteresse[('geracoes-')])
	ascendente.write('%d;' % verticeInteresse[('orientacoes-')])
	ascendente.write('\n')
ascendente.close()
db.shutdown()

