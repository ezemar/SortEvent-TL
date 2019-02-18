
from datetime import datetime

import os,inspect,json,ast,time

start = time.time()


fileName='589df9a3e4b0102b82ce3ffd.5c51ea76e7a45913a07d7ab6-'

#filename to be read

userToken='da788d47c6be083179ef039437b8ed567ec4d57d'
#userToken to looking for

uniqueDeviceId='00D00F0D-7C83-46DF-BFE0-BB2BA15F6FCC'
#device identificator

order='asc'
#order options 'asc' or 'desc'

criteria='eventUTCDate'
#order by 'eventUTCDate' or 'sequenceNumber'

debug=1

deleteDuplicates=1

color_list= [['HEADER','\033[0;90;40m'],['OKBLUE', '\033[94m'],['OKGREEN','\033[92m'],['WARNING','\033[93m'],['FAIL','\033[91m'],['ENDC','\033[0m'],['BOLD','\033[1m'],['UNDERLINE','\033[4m']]

def leftPad(char,length,text):
	if len(text)< length:
		res= char*(length-len(text))+text
	else:
		res=text
			
	return res
 
def color(conditionType,string):
	return color_list[conditionType][1]+str(string)+color_list[5][1]
	 
def getTime():
	t = datetime.now()
	return (str(t.hour),str(t.minute),str(t.second))

def getDate():
	t = datetime.now()
	return (str(t.day),str(t.month),str(t.year))

def readFile(fn):
	linesT=[]
	try:
		
		for x in range(4):
	
			print 'Cargando el Archivo '+fn+str(x)+'.log'
		
			with open(fn+str(x)+'.log') as f:
				lines = f.readlines()
				lines = [line.rstrip('\n') for line in open(fn+str(x)+'.log')]
				linesT +=lines
				
			print 'Archivo '+fn+str(x)+' cargado. OK!'
		
		print ''
		print 'El merge de worker-logs. contiene',color(2,len(linesT)),'lineas en total'
		
	except Exception,e:
		print 'No se puede leer el archivo '+fn+str(x)+'.log'+' ERROR!: ',e
		
	return linesT

def cutter(from_p,to_p,text_p):
	return text_p[text_p.index(from_p):text_p.index(to_p)+len(to_p)]

def collectJEvents(readedContent,userToken,deviceUniqueId):
	events=[]
	print ''
	try:
		if debug:
			print 'Tipos de Arrays de Events detectados'#si es SINGLE-LINE el array event tiene su principio y su final. Caso contratio el Array ocupa mas de un linea y se considera MULTI-LINE 
		for linea in range(len(readedContent)):	
			if userToken in readedContent[linea] and uniqueDeviceId in readedContent[linea]:
				#print 'userToken and uniqueDeviceId FLAG'
				if ('{"events"' in readedContent[linea] and '{"eventName"' in readedContent[linea]):
					#print 'event start FLAG'
					
					if ']}' in readedContent[linea]:
						#print 'single-line: converting event to jsonObject'
						events.append(json.loads('['+readedContent[linea]+']'))
						
						if debug:
							print 'SINGLE-LINE EVENT ARRAY. L: '+ color(2,linea+1)+' LEN: '+color(2,len(readedContent[linea]))+color(2,' STARTING: '+readedContent[linea][0:40]+'...')+' ENDING: '+color(2,readedContent[linea][-10:])
					
					else:
						#print 'multi-line: converting event to jsonObject'
						multiLineEventStringPL=''
						
						plusLine=0
						
						while not ']}' in readedContent[linea+plusLine]:
							plusLine+=2
							#print 'multi-line event. evaludar el fin del evento en linea'+str([linea+plusLine])
					
						for x in range(1,plusLine):
							
							multiLineEventStringPL+=readedContent[linea+2]
							#print color(2,multiLineEventString)
						
						multiLineEventString = readedContent[linea]+multiLineEventStringPL
						
						events.append(json.loads('['+multiLineEventString+']'))
						
						if debug:
							print 'MULTI-LINE EVENT ARRAY. SI: '+ color(2,linea+1)+' FL: '+color(2,linea+1+plusLine)+' LEN: '+color(2,len(multiLineEventString))+color(2,' STARTING: '+multiLineEventString[0:40]+'...')+' ENDING: '+color(2,multiLineEventString[-10:])
		
		print 'Coleccion de eventos en Objetos json creada'
		
		if debug:
			print 'Se encontraron',len(events),' Arrays "events"'
		
	except Exception, e:
		print 'Error al crear el array de objetos Json con el atributo '+criteria+ ' Exception is: ',e
	return events

def sortJEventsBy(jsonObjectEventList,orderByAttribute,orderSelector):
	
	try:
			
		singleEventListWSK=[]
		result=''
		mCount = 0
		for jsonObjectEvent in jsonObjectEventList:
			for eventsList in jsonObjectEvent:
				#print eventsList
				for eventIndex in range(len(eventsList.get('events'))):
					event = eventsList.get('events')[eventIndex]
	
					if event.get('userToken')==userToken and event.get('deviceSegment').get('uniqueDeviceID')==uniqueDeviceId:
						
						if deleteDuplicates:
							count=0
							
							for collection in singleEventListWSK:
								if collection[0].get('eventName') == event.get('eventName') and collection[1] == event.get('eventUTCDate'):
									count+=1
									mCount+=count
							
							if count == 0:
								singleEventListWSK.append([event,event.get('eventUTCDate')])
						else:
							singleEventListWSK.append([event,event.get('eventUTCDate')])

		print 'Tomar todos eventos anidados: OK! Existen',mCount,'duplicados!'
		
		if deleteDuplicates:
			print 'Eliminar duplicados: OK! Se eliminaron',mCount,'duplicados!'
		
		if orderSelector=='desc':
			
			if orderByAttribute =='sequenceNumber':
				result= sorted(singleEventListWSK, key = lambda x: int(x[1]), reverse=True)
			
			elif orderByAttribute=='eventUTCDate':
				result= sorted(singleEventListWSK,key=lambda x: datetime.strptime((x[1][0:18]), '%Y-%m-%dT%H:%M:%S'), reverse=True)
				
		elif orderSelector=='asc':
			
			if orderByAttribute =='sequenceNumber':
				result= sorted(singleEventListWSK, key = lambda x: int(x[1]), reverse=False)
			
			elif orderByAttribute=='eventUTCDate':	
				result= sorted(singleEventListWSK,key=lambda x: datetime.strptime((x[1][0:18]), '%Y-%m-%dT%H:%M:%S'), reverse=False)
		
	except Exception,e:
		print 'sortJEventsBy ERROR. Exception is:',e
		
	return result
	
	
def writeFile(text,filen):
	try:
		fi = open(filen, 'a')
		fi.write(text+'\n')
	except Exception,e:
		print 'No se puede escribir en el archivo ERROR: ',e
		
def loadHeader(text,ancho=40,character='*'):
	qr = (ancho/2)-len(text)/2
	qi = ancho-qr-len(text)

	print color(3,character*ancho)
	print color(3,character*qr +text+(character*qi))
	print color(3,character*ancho)


loadHeader('  SortEvent-TL v0.3 - GlobalLogic - 2019  ',100,':')

logo="""
+++++++++    ++++    ++++    ++++
+++++++++  ++++++++   ++++  ++++
++++      ++++  ++++   ++++++++
+++++++   ++++  ++++    ++++++
+++++++   ++++  ++++   ++++++++
++++       ++++++++   ++++  ++++
++++        ++++++   ++++    ++++
"""

print color(4,logo)	

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
		
		#eventCollection=collectEvents(readedData,userToken,uniqueDeviceId)
	
		#orderedEventCollection=sortByCriteria(eventCollection,criteria,order)#se ordena por sequenceNumber o fecha
	
		#filePath=dirName+'/SortEvent-TL_'+userToken[-10:]+'_'+timeStamp+'_'+criteria+'_'+order+'.log'
		
		#for x in range(len(orderedEventCollection)):
		#	writeFile(orderedEventCollection[x][0],filePath)
		#	writeFile(' ',filePath)
		
		#print ''	
		#print 'Operacion Completada. OK. Ordenado en archivo '+color(3,filePath)
		
		#print ''
		#print 'json object creation step'
		
		jsonObjectEventCollection=collectJEvents(readedData,userToken,uniqueDeviceId)
		
		print ''
		
		eventOccurrency=0
		
		if debug:
			print 'Coleccion JSON en BRUTO con userToken',userToken,'y uniqueDeviceID',uniqueDeviceId
			print ''
			
			for jsonObjectEvent in jsonObjectEventCollection:
				for eventsList in jsonObjectEvent:
					#print eventsList
					for eventIndex in range(len(eventsList.get('events'))):
						event = eventsList.get('events')[eventIndex]
		
						if event.get('userToken')==userToken and event.get('deviceSegment').get('uniqueDeviceID')==uniqueDeviceId:
							eventOccurrency+=1
							
							print 'eventName: '+color(3,event.get('eventName'))+' userToken: '+color(3,event.get('userToken'))+' uniqueDeviceID: '+color(3,event.get('deviceSegment').get('uniqueDeviceID'))+' eventUTCDate: '+color(3,event.get('eventUTCDate'))
		
			print ''
			print 'Se detectaron ',eventOccurrency,'eventos en total con userToken',userToken,'y uniqueDeviceID',uniqueDeviceId	
		
		print 'sortJEvent...'
		print ''		
		
		sortedJEvents=sortJEventsBy(jsonObjectEventCollection,criteria,order)
		
		print 'Los eventos unicos son: ',len(sortedJEvents) 
		print ''
		for event in sortedJEvents:
			print color(2,event[0].get('eventName')),color(3,event[1])
		
	except Exception,e:
		print 'Error al ejecutar el proceso principal',e
		


mainProcess(criteria,order,userToken,fileName)

end = time.time()
print ''
print 'Tarea ralizada en: ',color(3,str(round(end - start,1))+' segundos')

