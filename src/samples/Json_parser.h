#ifndef __JSON_PARSER__
#define __JOSN_PARSER__

struct send_scan{
    const char* msgId;
    const char* gatewayId;
    struct send_scan_source{
	const char* type;
        const char* userId;
    }source;
    struct send_scan_cmd{
	const char* type;
    }cmd;
    unsigned long long timestamp; 
};

struct send_brightness_cmd_value{
   char* zigbeeId; 	
   int brightness;
}send_brightness_cmd_value;

struct send_brightness{
    const char* msgId;
    const char* gatewayId;
    struct send_brightness_source{
	const char* type;
        const char* userId;
    }source;
    struct send_brightness_cmd{
	const char* type;
        struct send_brightness_cmd_value* value;
    }cmd;
    unsigned long long timestamp; 
};

struct report_scan_report_value{
     char zigbeeId[30];
     char zigbeeAddress[16];
     int voltage;
     int current;
     int brightness;
     double longitue;
     double latitude;
     int rssi;
     int pid;
}report_scan_report_value;

struct report_scan{
    char msgId[100];
    struct report_scan_source{
	char type[10];
        char gatewayId[30];
    }source;
    struct report_scan_report{
	char type[10];
	struct report_scan_report_value *value;	
    }report;
    unsigned long long timestamp; 
};

struct report_brightness_report_value{
     char zigbeeId[50];
     char zigbeeAddress[16];
     int voltage;
     int current;
     int brightness;
     double longitue;
     double latitude;
     int rssi;
     int pid;
}report_brightness_report_value;

struct report_brightness{
    char msgId[100];
    struct report_brightness_source{
	char type[10];
        char gatewayId[30];
    }source;
    struct report_brightness_report{
	char type[10];
	struct report_brightness_report_value *value;	
    }report;
    unsigned long long timestamp; 
};

#endif
