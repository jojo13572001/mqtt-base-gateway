/*******************************************************************************
 * Copyright (c) 2012, 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *   http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial contribution
 *******************************************************************************/

#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "MQTTClient.h"
#include "json.h"
#include "queue.h"
#include "Json_parser.h"
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>


//#include "parse_flags.h"

#define ADDRESS     "tcp://52.41.92.69:1883"
#define SUBTOPIC       "/gateway/command/"
#define PUBTOPIC       "/gateway/report/"
#define QOS        2 
#define TIMEOUT     10000L
#define CHECK_COND(cond) if (bool_compare_and_swap(&cond,1,1)) break;
#define SWAP_COND(cond,a,b) while(1){if (bool_compare_and_swap(&cond,a,b)) break; }

volatile MQTTClient_deliveryToken deliveredtoken;

typedef struct {
  int priority;
  char* message;
}msg;

typedef struct{
   int clientId;
   char gatewayId[30];
   int lightNum;
   volatile long *cond;
   queue_t *q;
   MQTTClient *client;
}consumer_type;

DEFINE_Q_GET(queue_get_msg, msg)
DEFINE_Q_DESTROY(queue_destroy_complete_msg, msg)

inline long val_compare_and_swap(volatile long *ptr, long old, long _new) {
    long prev;
    asm volatile("lock;"
#if defined(__amd64__)
                 "cmpxchgq %1, %2;"
#else
                 "cmpxchgl %1, %2;"
#endif
                 : "=a"(prev)
                 : "q"(_new), "m"(*ptr), "a"(old)
                 : "memory");
    return prev;
}

inline int bool_compare_and_swap(volatile long *ptr, long old, long new) {
    return val_compare_and_swap(ptr, old, new) == old;
}

void delivered(void *context, MQTTClient_deliveryToken dt)
{
    printf("ThreadId %u, Message with token value %d delivery confirmed\n", (unsigned int)pthread_self(), dt);
    deliveredtoken = dt;
}

int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
    int i, rc=1;
    json_object *payload, *cmd, *type;
    const char* type_str;
    printf("ThreadId %u, Message arrived\n", (unsigned int)pthread_self());
    printf("topic: %s\n", topicName);
    //printf("ThreadId %u received message=%s\n",(unsigned int)pthread_self() , message->payload);
    
    queue_t* q = (queue_t*)context;
    msg *qmsg= (msg*) malloc(sizeof(msg)); 
    qmsg->priority = 1;
    qmsg->message = (char*) malloc(sizeof(char)*strlen(message->payload));
    memcpy(qmsg->message, message->payload, sizeof(char)*strlen(message->payload));
    queue_put(q, qmsg);
    
    //MQTTClient_freeMessage(&message);
    //MQTTClient_free(topicName);
    return rc;
}

int scanLight(MQTTClient *client, int clientId, struct send_scan* scan_obj, int lightNum)
{
    //execute and fill out struct
    int i=0;
    char zigbeeId[30]={0};
    struct report_scan report;
    //memcpy(report.msgId, scan_obj->msgId, sizeof(char)*strlen(scan_obj->msgId));
    memcpy(report.msgId, scan_obj->msgId, 37);
    printf("scan_obj %s msgId size %d\n",scan_obj->msgId, (int)sizeof(char)*(int)strlen(scan_obj->msgId));
    memcpy(report.source.type, "Gateway", sizeof(char)*sizeof("Gateway"));
    memcpy(report.source.gatewayId, scan_obj->gatewayId, sizeof(char)*strlen(scan_obj->gatewayId));
    memcpy(report.report.type, "scan", sizeof(char)*sizeof("scan"));
    report.report.value = malloc( sizeof(struct report_scan_report_value)*lightNum);
    memset(report.report.value, 0, sizeof(struct report_scan_report_value)*lightNum);

    report.timestamp = (unsigned long long)time(NULL);

    sprintf(zigbeeId,"AAAA100000000%03d",clientId);
    memcpy(report.report.value[0].zigbeeId, zigbeeId ,sizeof(char)*strlen(zigbeeId));
    memcpy(report.report.value[0].zigbeeAddress, "127.0.0.1" ,sizeof(char)*sizeof("127.0.0.1"));
    report.report.value[0].voltage = 10;
    report.report.value[0].current = 20;
    report.report.value[0].brightness = 100;
    report.report.value[0].longitue = 123.45678;
    report.report.value[0].latitude = 31.3456;
    report.report.value[0].rssi = 256;

    for(i=1;i<lightNum;i++){
	sprintf(zigbeeId,"BBBB100000000%03d",clientId+i);
    	memcpy(report.report.value[i].zigbeeId, zigbeeId ,sizeof(char)*strlen(zigbeeId));
    	memcpy(report.report.value[i].zigbeeAddress, "127.0.0.1" ,sizeof(char)*sizeof("127.0.0.1"));
    	report.report.value[i].voltage = 10;
    	report.report.value[i].current = 20;
    	report.report.value[i].brightness = 100;
    	report.report.value[i].longitue = 123.45678;
    	report.report.value[i].latitude = 31.3456;
    	report.report.value[i].rssi = 256;
    	report.report.value[i].pid = 256;
    }


    //transfer to json format
    json_object *light_object,*source_object,*report_object, *msgId_object;
    light_object = json_object_new_object();
    source_object = json_object_new_object();
    report_object = json_object_new_object();
    msgId_object = json_object_new_object();

    //msgId
    msgId_object = json_object_new_string(report.msgId);
    json_object_object_add(light_object, "msgId", msgId_object);

    //source
    json_object* source_type_object = json_object_new_string(report.source.type);
    json_object_object_add(source_object, "type", source_type_object);

    json_object* gatewayId_object = json_object_new_string(report.source.gatewayId);
    json_object_object_add(source_object, "gatewayId", gatewayId_object);

    json_object_object_add(light_object, "source", source_object);

    //report
    json_object* report_type_object = json_object_new_string(report.report.type);
    json_object_object_add(report_object, "type", report_type_object);

    json_object *value_array;
    value_array = json_object_new_array();
    for(i=0;i<lightNum;i++){
    	json_object *value_object = json_object_new_object();
	json_object *zigbeeId_object = json_object_new_string(report.report.value[i].zigbeeId);
	json_object *zigbeeAddress_object = json_object_new_string(report.report.value[i].zigbeeAddress);
	json_object *voltage_object = json_object_new_int(report.report.value[i].voltage);
	json_object *current_object = json_object_new_int(report.report.value[i].voltage);
	json_object *brightness_object = json_object_new_int(report.report.value[i].brightness);
	json_object *longitue_object = json_object_new_double(report.report.value[i].longitue);
	json_object *latitute_object = json_object_new_double(report.report.value[i].latitude);
	json_object *rssi_object = json_object_new_int(report.report.value[i].rssi);
	json_object *pid_object = json_object_new_int(report.report.value[i].pid);

	json_object_object_add(value_object,"zigbeeId", zigbeeId_object);
	json_object_object_add(value_object,"zigbeeAddress", zigbeeAddress_object);
	json_object_object_add(value_object,"voltage", voltage_object);
	json_object_object_add(value_object,"current", current_object);
	json_object_object_add(value_object, "brightness", brightness_object);
	json_object_object_add(value_object, "longitue", longitue_object);
	json_object_object_add(value_object, "latitute", latitute_object);
	json_object_object_add(value_object, "rssi", rssi_object);
	json_object_object_add(value_object, "pid", pid_object);

    	json_object_array_add(value_array, value_object);
    }

    free(report.report.value);

    json_object* timestamp_object = json_object_new_int64(report.timestamp); 
    json_object_object_add(report_object, "value",  value_array);
    json_object_object_add(light_object, "report",  report_object);
    json_object_object_add(light_object, "timestamp",  timestamp_object); 

    const char *Spayload =  json_object_to_json_string(light_object);
    
    //publish response message
    MQTTClient_message pubmsg = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int rc;
    pubmsg.payload = (void*) Spayload;
    pubmsg.payloadlen = sizeof(char)*strlen(Spayload);
    pubmsg.qos = QOS;
    pubmsg.retained = 0;
    deliveredtoken = 0;

    char pubTopic[100];
    sprintf(pubTopic, "%s%s", PUBTOPIC, scan_obj->gatewayId);
    //printf("scan start pub %s\n", pubTopic);
    if(MQTTClient_publishMessage(*client, pubTopic, &pubmsg, &token) != MQTTCLIENT_SUCCESS){
	printf("clientId %d publish message %s fail\n",clientId, pubTopic);
	return 0;	
    }
    printf("ClientId %d Waiting for publication of %s\n"
            "on topic %s\n",
            clientId, Spayload, pubTopic);

    
    //json_object_array_del_idx(value_array, 0, lightNum);
    json_object_put(value_array);
    json_object_put(source_object);
    json_object_put(report_object);
    json_object_put(msgId_object);
    json_object_put(light_object);

    return 1;	
}

int brightLight(MQTTClient *client, int clientId, struct send_brightness* brightness_obj, int brightnessNum)
{
    //execute and fill out struct
    int i;
    char zigbeeId[30];
    struct report_brightness report;
    //memcpy(report.msgId, brightness_obj->msgId, sizeof(char)*strlen(brightness_obj->msgId));
    memcpy(report.msgId, brightness_obj->msgId, 37);
    memcpy(report.source.type, "Gateway", sizeof(char)*sizeof("Gateway"));
    memcpy(report.source.gatewayId, brightness_obj->gatewayId, sizeof(char)*sizeof(brightness_obj->gatewayId));
    memcpy(report.report.type, "brightness", sizeof(char)*sizeof("brightness"));
    report.report.value = malloc( sizeof(report_brightness_report_value)*brightnessNum);

    report.timestamp = (unsigned long long)time(NULL);

    for(i=0;i<brightnessNum;i++){
	sprintf(zigbeeId,"%s",brightness_obj->cmd.value[i].zigbeeId);
    	memcpy(report.report.value[i].zigbeeId, zigbeeId ,sizeof(zigbeeId));
    	memcpy(report.report.value[i].zigbeeAddress, "127.0.0.1" ,sizeof("127.0.0.1"));
    	report.report.value[i].voltage = 10;
    	report.report.value[i].current = 20;
    	report.report.value[i].brightness = 100;
    	report.report.value[i].longitue = 123.45678;
    	report.report.value[i].latitude = 31.3456;
    	report.report.value[i].rssi = 256;
    	report.report.value[i].pid = 256;
    }


    //transfer to json format
    json_object *light_object,*source_object,*report_object, *msgId_object;
    light_object = json_object_new_object();
    source_object = json_object_new_object();
    report_object = json_object_new_object();
    msgId_object = json_object_new_object();

    //msgId
    msgId_object = json_object_new_string(report.msgId);
    json_object_object_add(light_object, "msgId", msgId_object);

    //source
    json_object* source_type_object = json_object_new_string(report.source.type);
    json_object_object_add(source_object, "type", source_type_object);

    json_object* gatewayId_object = json_object_new_string(report.source.gatewayId);
    json_object_object_add(source_object, "gatewayId", gatewayId_object);

    json_object_object_add(light_object, "source", source_object);

    //report
    json_object* report_type_object = json_object_new_string(report.report.type);
    json_object_object_add(report_object, "type", report_type_object);

    json_object *value_array;
    value_array = json_object_new_array();
    for(i=0;i<brightnessNum;i++){
    	json_object *value_object = json_object_new_object();
	json_object *zigbeeId_object = json_object_new_string(report.report.value[i].zigbeeId);
	json_object *zigbeeAddress_object = json_object_new_string(report.report.value[i].zigbeeAddress);
	json_object *voltage_object = json_object_new_int(report.report.value[i].voltage);
	json_object *current_object = json_object_new_int(report.report.value[i].current);
	json_object *brightness_object = json_object_new_int(report.report.value[i].brightness);
	json_object *longitue_object = json_object_new_double(report.report.value[i].longitue);
	json_object *latitute_object = json_object_new_double(report.report.value[i].latitude);
	json_object *rssi_object = json_object_new_int(report.report.value[i].rssi);
	json_object *pid_object = json_object_new_int(report.report.value[i].pid);

	json_object_object_add(value_object,"zigbeeId", zigbeeId_object);
	json_object_object_add(value_object,"zigbeeAddress", zigbeeAddress_object);
	json_object_object_add(value_object,"voltage", voltage_object);
	json_object_object_add(value_object,"current", current_object);
	json_object_object_add(value_object, "brightness", brightness_object);
	json_object_object_add(value_object, "longitue", longitue_object);
	json_object_object_add(value_object, "latitute", latitute_object);
	json_object_object_add(value_object, "rssi", rssi_object);
	json_object_object_add(value_object, "pid", pid_object);

    	json_object_array_add(value_array, value_object);
    }

    free(report.report.value);

    json_object* timestamp_object = json_object_new_int64(report.timestamp); 
    json_object_object_add(report_object, "value",  value_array);
    json_object_object_add(light_object, "report",  report_object);
    json_object_object_add(light_object, "timestamp",  timestamp_object); 

    const char *Spayload =  json_object_to_json_string(light_object);
    
    //publish response message
    MQTTClient_message pubmsg = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int rc;
    pubmsg.payload = (void*) Spayload;
    pubmsg.payloadlen = sizeof(char)*strlen(Spayload);
    pubmsg.qos = QOS;
    pubmsg.retained = 0;
    deliveredtoken = 0;

    char pubTopic[100];
    sprintf(pubTopic, "%s%s", PUBTOPIC, brightness_obj->gatewayId);
    if(MQTTClient_publishMessage(*client, pubTopic, &pubmsg, &token) != MQTTCLIENT_SUCCESS){
	printf("clientId %d publish message %s fail\n",clientId, pubTopic);
	return 0;	
    }
    printf("ClientId %d Waiting for publication of %s\n"
            "on topic %s\n",
            clientId, Spayload, pubTopic);

    
    //json_object_array_del_idx(value_array, 0, brightnessNum);
    json_object_put(value_array);
    json_object_put(source_object);
    json_object_put(report_object);
    json_object_put(msgId_object);
    json_object_put(light_object);

    return 1;	
}

int schedule(MQTTClient *client, int clientId, const char* msgId)
{
	return 1;	
}
void connlost(void *context, char *cause)
{
    printf("\nConnection lost\n");
    printf("     cause: %s\n", cause);
}

void free_msg(msg *t) {
	if(t->message != NULL)
		free(t->message);
	free(t);
}

int parse_msg(int clientId, msg *qmsg, char *type){
    json_object *payload, *cmd, *cmd_type;
    const char* Scmd_type;
    payload = json_tokener_parse(qmsg->message);
    if (payload == NULL) {
    	printf("parser clientId %d consumer %u parse incoming json message fail\n", clientId, (unsigned int)pthread_self());
	return 0;
    }

    cmd = json_object_object_get(payload, "cmd");
    if (cmd != NULL) {
	cmd_type = json_object_object_get(cmd, "type");
    	if (cmd_type != NULL) {
    		Scmd_type = json_object_get_string(cmd_type);
                memcpy(type, Scmd_type, sizeof(char)*strlen(Scmd_type));
    	}else{
    		printf("clientId %d consumer %u parse cmd fail\n", clientId, (unsigned int)pthread_self());
    		json_object_put(payload);
		return 0;
	}
    }else{
    	printf("clientId %d consumer %u parse cmd fail\n", clientId, (unsigned int)pthread_self());
    	json_object_put(payload);
	return 0;
    }

    json_object_put(payload);
    return 1;
}


int parse_brightness_msg(int clientId, msg *qmsg, struct send_brightness* brightness_obj, int* brightnessNum, json_object** payload){
    json_object *msgId,*gatewayId,*source, *cmd, *timestamp, *type, *source_type,*source_userId,  *cmd_type, *cmd_value;
    const char *SmsgId, *SgatewayId, *Ssource_type, *Ssource_userId, *Scmd_type, *Stimestamp;
    unsigned long long Itimestamp;

    *payload = json_tokener_parse(qmsg->message);
    if (*payload == NULL) {
    	printf("brightness clientId %d consumer %u parse incoming json message fail\n", clientId, (unsigned int)pthread_self());
	return 0;
    }

    msgId = json_object_object_get(*payload, "msgId");
    if (msgId != NULL) {
    	SmsgId = json_object_get_string(msgId);
        brightness_obj->msgId = SmsgId;
    }else{
    	printf("clientId %d consumer %d threadId %u parse msgId fail\n",clientId, clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }

    gatewayId = json_object_object_get(*payload, "gatewayId");
    if (gatewayId != NULL) {
    	SgatewayId = json_object_get_string(gatewayId);
        brightness_obj->gatewayId = SgatewayId;
    }else{
    	printf("clientId %d consumer %d threadId %u parse gatewayId fail\n",clientId, clientId, (unsigned int)pthread_self());
    	//json_object_put(*payload);
	return 0;
    } 

    source = json_object_object_get(*payload, "source");
    if (source != NULL) {
	source_type = json_object_object_get(source, "type");
	source_userId = json_object_object_get(source, "userId");
    	if (source_type != NULL && source_userId != NULL) {
    		Ssource_type = json_object_get_string(source_type);
    		Ssource_userId = json_object_get_string(source_userId);
        	brightness_obj->source.type = Ssource_type;
        	brightness_obj->source.userId = Ssource_userId;
    	}else{
    		printf("clientId %d consumer %d threadId %u parse source fail\n",clientId, clientId, (unsigned int)pthread_self());
    		//json_object_put(payload);
		return 0;
	}
    }else{
    	printf("clientId %d comsuer %u parse source fail\n", clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }    

    cmd = json_object_object_get(*payload, "cmd");
    if (cmd != NULL) {
	cmd_type = json_object_object_get(cmd, "type");
    	if (cmd_type != NULL) {
    		Scmd_type = json_object_get_string(cmd_type);
        	brightness_obj->cmd.type = Scmd_type;
	}else{
    		printf("clientId %d consumer %d threadId %u parse cmd_type fail\n",clientId, clientId, (unsigned int)pthread_self());
    		//json_object_put(payload);
		return 0;
	}
        //parse array value
	cmd_value = json_object_object_get(cmd, "value");
        unsigned long long i;
	if(cmd_value == NULL){
    		printf("clientId %d consumer %d threadId %u parse cmd_value fail\n",clientId, clientId, (unsigned int)pthread_self());
    		//json_object_put(payload);
		return 0;
	}
	*brightnessNum = json_object_array_length(cmd_value);
       	brightness_obj->cmd.value = malloc(sizeof(struct send_brightness_cmd_value)* *brightnessNum);
        for(i = 0; i < json_object_array_length(cmd_value); i++)
	{
		json_object *obj = json_object_array_get_idx(cmd_value, i);
    		if (obj == NULL) {
    			printf("clientId %d consumer %d threadId %u get idx cmd_value fail\n",clientId, clientId, (unsigned int)pthread_self());
    			//json_object_put(payload);
			return 0;
		}
    		json_object* zigbeeId = json_object_object_get(obj, "zigbeeId");
    		if (zigbeeId != NULL) {
    			const char* SzigbeeId = json_object_get_string(zigbeeId);
       			brightness_obj->cmd.value[i].zigbeeId = malloc(sizeof(char)* strlen(SzigbeeId));
       			memcpy(brightness_obj->cmd.value[i].zigbeeId, SzigbeeId, sizeof(char)* strlen(SzigbeeId));
    			//printf("clientId %d comsumer %u parse cmd_value_zigbeeId %s\n", clientId, (unsigned int)pthread_self(), SzigbeeId);
		}else{
    			printf("clientId %d consumer %d threadId %u parse cmd_value_zigbeeId fail\n",clientId, clientId, (unsigned int)pthread_self());
    			//json_object_put(payload);
			return 0;
		}
    		json_object* brightness = json_object_object_get(obj, "brightness");

    		if (brightness != NULL) {
    			int Ibrightness = json_object_get_int(brightness);
       			brightness_obj->cmd.value[i].brightness = Ibrightness;
    			//printf("clientId %d comsumer %u parse cmd_value_brightness %d\n", clientId, (unsigned int)pthread_self(), Ibrightness);
		}else{
    			printf("clientId %d consumer %d threadId %u parse cmd_value_brghtness fail\n",clientId, clientId, (unsigned int)pthread_self());
    			//json_object_put(payload);
			return 0;
		}
	}

    }else{
   	printf("clientId %d consumer %d threadId %u parse cmd fail\n",clientId, clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }

    timestamp = json_object_object_get(*payload, "timestamp");
    if (timestamp != NULL) {
    	Itimestamp = json_object_get_int64(timestamp);
       	brightness_obj->timestamp = Itimestamp;
    }else{
    	printf("clientId %d consumer %d threadId %u parse timestamp fail\n",clientId, clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }
    //json_object_put(payload);
    return 1;
}

int parse_scan_msg(int clientId, msg *qmsg, struct send_scan* scan_obj, json_object** payload){
    json_object *msgId,*gatewayId,*source, *cmd, *timestamp, *type, *source_type,*source_userId,  *cmd_type;
    const char *SmsgId, *SgatewayId, *Ssource_type, *Ssource_userId, *Scmd_type, *Stimestamp;
    unsigned long long Itimestamp;

    *payload = json_tokener_parse(qmsg->message);
    if (*payload == NULL) {
    	printf("clientId %d consumer %u parse incoming json message fail\n", clientId, (unsigned int)pthread_self());
	return 0;
    }

    msgId = json_object_object_get(*payload, "msgId");
    if (msgId != NULL) {
    	SmsgId = json_object_get_string(msgId);
        scan_obj->msgId = SmsgId;
    }else{
    	printf("consumer %d threadId %u parse msgId fail\n", clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }

    gatewayId = json_object_object_get(*payload, "gatewayId");
    if (gatewayId != NULL) {
    	SgatewayId = json_object_get_string(gatewayId);
        scan_obj->gatewayId = SgatewayId;
    }else{
    	printf("clientId %d consumer %u parse gatewayId fail\n", clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    } 
    
    source = json_object_object_get(*payload, "source");
    if (source != NULL) {
	source_type = json_object_object_get(source, "type");
	source_userId = json_object_object_get(source, "userId");
    	if (source_type != NULL && source_userId != NULL) {
    		Ssource_type = json_object_get_string(source_type);
    		Ssource_userId = json_object_get_string(source_userId);
        	scan_obj->source.type = Ssource_type;
        	scan_obj->source.userId = Ssource_userId;
    	}else{
    		printf("clientId %d consumer %u parse source fail\n", clientId, (unsigned int)pthread_self());
    		//json_object_put(payload);
		return 0;
	}
    }else{
    	printf("clientId %d comsuer %u parse source fail\n", clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }

    cmd = json_object_object_get(*payload, "cmd");
    if (cmd != NULL) {
	cmd_type = json_object_object_get(cmd, "type");
    	if (cmd_type != NULL) {
    		Scmd_type = json_object_get_string(cmd_type);
        	scan_obj->cmd.type = Scmd_type;
	}else{
    		printf("clientId %d comsumer %u parse cmd fail\n", clientId, (unsigned int)pthread_self());
    		//json_object_put(payload);
		return 0;
	}
    }else{
    	printf("clientId %d comsumer %u parse cmd fail\n", clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }

    timestamp = json_object_object_get(*payload, "timestamp");
    if (timestamp != NULL) {
    	Itimestamp = json_object_get_int64(timestamp);
       	scan_obj->timestamp = Itimestamp;
    }else{
    	printf("clientId %d comsumer %u parse timestamp fail\n", clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }

    printf("clientId %d comsumer %u cmd type=%s\n",clientId, (unsigned int)pthread_self(), Scmd_type);

    if (!SmsgId|| !SgatewayId|| !Ssource_type|| !Ssource_userId|| !Scmd_type|| !Itimestamp) {
	printf("clientId %d comsumer %u Json string parse failed\n", clientId, (unsigned int)pthread_self());
    	//json_object_put(payload);
	return 0;
    }
    
    return 1;
}



void *consumer(void *arg){
  msg *qmsg=NULL;
  consumer_type* consumer_data = (consumer_type*) arg;
  MQTTClient *client = consumer_data->client;
  int clientId = consumer_data->clientId;
  queue_t *q = consumer_data->q;
  for(;;){
    queue_get_msg(q, &qmsg);

    if (qmsg != NULL){
      printf("\n consumer%u consume %s\n",(unsigned int)pthread_self(), (char *)qmsg->message);
      struct send_scan scan_obj;
      struct send_brightness brightness_obj;
      char type[30]={0};
      json_object **payload = malloc(sizeof(json_object*));
	
      if(parse_msg(clientId, qmsg,type)==1){
    	 if (strcmp(type, "scan") == 0 ){
		printf("clientId %d threadId %u start parse scan msg\n" ,clientId, (unsigned int)pthread_self());
		if(parse_scan_msg(clientId, qmsg, &scan_obj, payload) == 1){
		   printf("clientId %d consumer %u parse scan msg msgId%s\n" ,clientId, (unsigned int)pthread_self(),scan_obj.msgId);
	           scanLight(client, clientId, &scan_obj, consumer_data->lightNum);
      		}else{
		   printf("clientId %d consumer %u parse scan msg fail!\n" ,clientId, (unsigned int)pthread_self());
      		}
			
	    }else if (strcmp(type, "brightness") == 0 ){
		int brightnessNum=0;
		if(parse_brightness_msg(clientId, qmsg, &brightness_obj, &brightnessNum, payload) == 1){
		   printf("clientId %d consumer %u parse brightness msg successfully\n" ,clientId, (unsigned int)pthread_self());
	           brightLight(client, clientId, &brightness_obj, brightnessNum);
      		}else{
		   printf("clientId %d consumer %u parse brightness msg fail!\n" ,clientId, (unsigned int)pthread_self());
      		}
	    }else if (strcmp(type, "schedule") == 0 ){
        	//rc = schedule();
    	 }
      }else{
      	printf("clientId %d consumer %u parse msg fail!\n",clientId ,(unsigned int)pthread_self());
      }
      if(*payload != NULL){
    	json_object_put(*payload);
      }
      free(payload);
      free_msg(qmsg);
    }else{
      //printf("clientId %d consumer %u retireve msg NULL!\n" ,clientId, (unsigned int)pthread_self());
    }

    sleep(1);
    CHECK_COND(*consumer_data->cond);
  }
  printf("clientId %d finish consumer %u\n", clientId,  (unsigned int)pthread_self());
  
}

int cmp_int_ptr(msg *a, msg *b) {
        if(a->priority < b->priority)
                return -1;
        else if(a->priority > b->priority)
                return 1;
        else
                return 0;
}

typedef struct{
    int clientId;
    char gatewayId[30];
    int consumerNum;
    int queueSize;
    int lightNum;
    volatile long* cond;
}simulator_type;

void *simulator(void *arg){
    simulator_type *simulator = (simulator_type*) arg;
    MQTTClient client;
    pthread_t t[10];
    consumer_type consumer_data[simulator->consumerNum];
    volatile long cond = 0;
    int i=0;
    int rc=0;
    printf("create simulator %u, clientId %d\n", (unsigned int)pthread_self(), simulator->clientId);

    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    conn_opts.username = "oring-gateway@oring-networking.com";
    conn_opts.password = "oring28942349";
    
    queue_t *q;
    q = queue_create_limited_sorted(simulator->queueSize, 1, (int (*)(void *, void *))cmp_int_ptr);

    for(i=0; i<simulator->consumerNum; i++){
    	consumer_data[i].clientId = simulator->clientId;
    	consumer_data[i].lightNum = simulator->lightNum;
    	consumer_data[i].cond = &cond; 
    	consumer_data[i].q = q; 
    	consumer_data[i].client = &client; 
    	pthread_create(&t[i],NULL,consumer,(void *)&consumer_data[i]);
    }
     
    //create MQTT
    char clientIdStr[25];
    sprintf(clientIdStr, "%d", simulator->clientId);
    MQTTClient_create(&client, ADDRESS, clientIdStr,  MQTTCLIENT_PERSISTENCE_NONE, NULL);
    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    MQTTClient_setCallbacks(client, q, connlost, msgarrvd, delivered);

    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to connect, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }

    char subTopic[100];
    sprintf(subTopic, "%s%s", SUBTOPIC, simulator->gatewayId);
    MQTTClient_subscribe(client, subTopic, QOS);

    printf("simulator %u subscribing to topic %s\nfor client %s using QoS%d\n\n"
           ,(unsigned int)pthread_self(), subTopic, clientIdStr, QOS);

    while(1) 
    {
	sleep(1);
    	//printf("simulator%u wait for consumer\n", (unsigned int)pthread_self());
    	CHECK_COND(*simulator->cond);
    }
     
    MQTTClient_disconnect(client, 10000);
    MQTTClient_destroy(&client);

    //notify consumer to terminate 
    SWAP_COND(cond,0,1);
    for(i=0; i<simulator->consumerNum; i++){
    	pthread_join(t[i],NULL);
    } 
    queue_destroy_complete_msg(q, free_msg);

    printf("finish simulator%u\n", (unsigned int)pthread_self());
}

/*
1. Thread num
2. consumer num
3. consuming time 
4. queue size
5. queue consuming speed
*/


int main(int argc, char* argv[])
{
  int simulatorNum = 10;
  pthread_t t[simulatorNum];
  volatile long simulatorCond = 0;
  int i =0;
  int ch;
  //create message queue

  simulator_type simulatorArray[simulatorNum];
  for(i=0; i<simulatorNum; i++){
	int index = i*100+1;
        simulatorArray[i].clientId = index;
        sprintf(simulatorArray[i].gatewayId, "0000100000000%03d", index);
        simulatorArray[i].consumerNum = 1;
        simulatorArray[i].lightNum = 100;
        simulatorArray[i].queueSize = 10;
        simulatorArray[i].cond = &simulatorCond;
    	pthread_create(&t[i],NULL,simulator,(void *)&simulatorArray[i]);
  }
  printf("press q Enter to Stop\n");
  do 
  {
      ch = getchar();
  } while(ch!='Q' && ch != 'q');

  SWAP_COND(simulatorCond,0,1);

  for(i=0; i<simulatorNum; i++){
    	pthread_join(t[i],NULL);
  }   
  return 1;
}
