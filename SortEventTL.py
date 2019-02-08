from datetime import datetime
import os

fileName='Ejemplo.log'
#filename to be read

userToken='823d170213fb04625e9bb198295ea2ef18f288dc'
#userToken to looking for

order='desc'
#order options 'asc' or 'desc'

criteria='eventUTCDate'
#order by 'eventUTCDate' or 'sequenceNumber'

debug=0

color= [['HEADER','\033[0;90;40m'],['OKBLUE', '\033[94m'],['OKGREEN','\033[92m'],['WARNING','\033[93m'],['FAIL','\033[91m'],['ENDC','\033[0m'],['BOLD','\033[1m'],['UNDERLINE','\033[4m']]

def leftPad(char,length,text):
	if len(text)< length:
		res= char*(length-len(text))+text
	else:
		res=text
			
	return res
 
def printc(conditionType,string):
	print color[conditionType][1]+string+color[5][1]
	 
def getTime():
	t = datetime.now()
	return (str(t.hour),str(t.minute),str(t.second))

def getDate():
	t = datetime.now()
	return (str(t.day),str(t.month),str(t.year))

def readFile(fn):
	try:
		print 'Cargando el Archivo '+fn
		with open(fn) as f:
			lines = f.readlines()
			lines = [line.rstrip('\n') for line in open(fn)]
		print 'Archivo '+fn+' cargado. OK!'
	except Exception,e:
		print 'No se puede leer el archivo ERROR!: ',e
		
	return lines
	
def collectEvents(readedContent,criteria):
	events=[]
	try:
		for index in range(len(readedContent)):	
			if criteria in readedContent[index]:
				if '{"events"' in readedContent[index]:
					events.append(readedContent[index])
	except:
		print 'Error al buscar eventos con criterio: '+criteria
	return events

def sortBySecondElement(arrayList):
	return arrayList[1:0]

def sortByCriteria(collectedEvents,criteria,order):
	print 'tienen que ordenarse',len(collectedEvents),'eventos'
	try:
		orderedEvents=[]
		while len(orderedEvents) < len(collectedEvents):
			for i in range(len(collectedEvents)):	
				if criteria in collectedEvents[i]:
					
					posicionInicial = collectedEvents[i].index(criteria)+len(criteria)+2
									
					modificador=0
					while True:
						
						if ',' in collectedEvents[i][posicionInicial:posicionInicial+modificador]:
							posicionFinal=posicionInicial+modificador-1
							
							break
						else:
							modificador+=1
									
					orderedEvents.append([collectedEvents[i],collectedEvents[i][posicionInicial:posicionFinal]])
			
		if debug:
			print 'Proceso de ordenamiento finalizado correctamente'
	except Exception,e:
		print 'Error al realizar el ordenamiento de eventos. Criterio['+criteria+' Exception is:'+e
	
	#if debug:
		#print sorted(orderedEvents, key = lambda x: int(x[1]))
	
	if order=='desc':
		if criteria =='sequenceNumber':
			return sorted(orderedEvents, key = lambda x: int(x[1]), reverse=True)
		
		elif criteria=='eventUTCDate':	
			return sorted(orderedEvents,key=lambda x: datetime.strptime((x[1][1:19]), '%Y-%m-%dT%H:%M:%S'), reverse=True)
			
	elif order=='asc':
		if criteria =='sequenceNumber':
			return sorted(orderedEvents, key = lambda x: int(x[1]), reverse=False)
		
		elif criteria=='eventUTCDate':	
			return sorted(orderedEvents,key=lambda x: datetime.strptime((x[1][1:19]), '%Y-%m-%dT%H:%M:%S'), reverse=False)
		
			
	
	else:
		return 'ERROR, la busqueda con ese criterio no esta implementada',criteria
	
	
def writeFile(text,filen):
	try:
		fi = open(filen, 'a')
		fi.write(text+'\n')
	except Exception,e:
		print 'No se puede escribir en el archivo ERROR: ',e
		
def loadHeader(text,ancho=40,character='*'):
	qr = (ancho/2)-len(text)/2
	qi = ancho-qr-len(text)

	printc(3,character*ancho)
	printc(3,character*qr +text+(character*qi))
	printc(3,character*ancho)


loadHeader('  SortEvent-TL v0.2 - GlobalLogic - 2019  ',100,':')

logo="""
+++++++++    ++++    ++++    ++++
+++++++++  ++++++++   ++++  ++++
++++      ++++  ++++   ++++++++
+++++++   ++++  ++++    ++++++
+++++++   ++++  ++++   ++++++++
++++       ++++++++   ++++  ++++
++++        ++++++   ++++    ++++
"""
printc(4,logo)	

try:
	dirName='Output'
	os.mkdir(dirName)
	print "Carpeta " , dirName ,  " fue creada "
except Exception,e:
	print "La Carpeta " , dirName ,  " Ya existia" 
	
def mainProcess(criteria,order,userToken,fileName):
	
	timeStamp= leftPad('0',2,getDate()[0])+'-'+leftPad('0',2,getDate()[1])+'-'+leftPad('0',4,getDate()[2])+'_'+leftPad('0',2,getTime()[0])+leftPad('0',2,getTime()[1])+leftPad('0',2,leftPad('0',2,getTime()[2]))
		
	try:		
		readedData=readFile(fileName)
		eventCollection=collectEvents(readedData,userToken)
		orderedEventCollection=sortByCriteria(eventCollection,criteria,order)#se ordena por sequenceNumber o fecha
		
		filePath=dirName+'/SortEvent-TL_'+userToken[-10:]+'_'+timeStamp+'_'+criteria+'_'+order+'.log'
		
		for x in range(len(orderedEventCollection)):
			writeFile(orderedEventCollection[x][0],filePath)
			writeFile(' ',filePath)
			
		print 'Operacion Completada. OK. Ordenado en archivo '+filePath
		
	except Exception,e:
		print 'Error al ejecutar el proceso principal',e
		


mainProcess(criteria,order,userToken,fileName)
