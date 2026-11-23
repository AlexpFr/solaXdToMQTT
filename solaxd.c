/*
    Daemon for communication with SolaX-X1_Mini inverter via RS485
    
    Copyright (C) 2020 - Jens Jordan
    
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*********************************************************************************************/

#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <termios.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdatomic.h>
#include <mosquitto.h>

/*** Defines ********************************************************************************************/

#define SOLARXD_STRING             "SolaXd"
#define VERSION_STRING             "Version 0.2.0 (07-Jan-2021)"

#define DEFAULT_TTY_DEVICE_NAME    "/dev/ttyUSB0"         // serial device
#define DEFAULT_TCP_PORT           6789                   // http-server port

#define DEFAULT_MQTT_SERVER        "localhost"            // MQTT-server adress
#define DEFAULT_MQTT_PORT          1883                   // MQTT-server port
#define DEFAULT_MQTT_LOGIN         "mqtt"                 // MQTT-server login
#define DEFAULT_MQTT_PASSWORD      "mqtt"                 // MQTT-server password

#define DEFAULT_AVERAGE_SAMPLES    10                     // interval use for average calculation (in seconds)
#define DEFAULT_INVERTER_ADDRESS   0x0A                   // must be unique in case of more inverters in the same RS485 bus
#define DEFAULT_LOG_FILE           NULL                   // stderr is used if NULL
#define DEFAULT_LOG_LEVEL          LOG_NOTICE             // support for different log levels
#define DEFAULT_TEST_MODE          0                      // enabled / disabled of test & debug code

#define QUALITY_OF_SERVICE_COUNT   100                    // QoS interval (in seconds)
#define TIMEOUT_INVERTER_ONLINE    30                     // in seconds
#define MAX_INDEX_OF_LIVE_DATA     (QUALITY_OF_SERVICE_COUNT - 1)

#define MQTT_CONNACK_TIMEOUT       5                      // wait for the answer of the broker to a connection request (in seconds)
#define MQTT_RETRY_INTERVAL        5                      // interval between connection attempts (in seconds)

/*** Macros ********************************************************************************************/

#define lowByte(i)    ( (uint8_t) i )
#define highByte(i)   ( (uint8_t) ( ((int) i) >> 8) )

#define ERROR_MESSAGE(...)   if (arg_LogLevel >= LOG_ERROR ) { log_Message(LOG_ERROR,  fp_log_file, __VA_ARGS__); }
#define NOTICE_MESSAGE(...)  if (arg_LogLevel >= LOG_NOTICE) { log_Message(LOG_NOTICE, fp_log_file, __VA_ARGS__); }
#define INFO_MESSAGE(...)    if (arg_LogLevel >= LOG_INFO  ) { log_Message(LOG_INFO,   fp_log_file, __VA_ARGS__); }
#define DEBUG_MESSAGE(...)   if (arg_LogLevel >= LOG_DEBUG ) { log_Message(LOG_DEBUG,  fp_log_file, __VA_ARGS__); }
#define TRACE_MESSAGE(...)   if (arg_LogLevel >= LOG_TRACE ) { log_Message(LOG_TRACE,  fp_log_file, __VA_ARGS__); }
#define LOG_USE_COLOR        // If the library is compiled with `-DLOG_USE_COLOR` ANSI color escape codes will be used when printing.

/*** Typedefs ******************************************************************************************/

typedef enum
{
    LOG_ERROR = 0,
    LOG_NOTICE,
    LOG_INFO,
    LOG_DEBUG,
    LOG_TRACE
} logLevel_t;

typedef enum
{
    STATE_BROARDCAST,
    STATE_INVERTER_ADDRESS,
    STATE_QUERY_LIVE_DATA
} Solax_StateQuery_t;

typedef enum
{
    ERR_FATAL = -1,     // read or write error on the serial interface
    ERR_NONE = 0,
    ERR_NO_DATA,
    ERR_INVALID_MSG,
    ERR_CRC_ERROR
} Solax_ErrorQuery_t;

typedef struct
{
    uint8_t Header[2];
    uint8_t Source[2];
    uint8_t Destination[2];
    uint8_t ControlCode;
    uint8_t FunctionCode;
    uint8_t DataLength;
    uint8_t Data[100];
} Solax_Message_t;

typedef struct
{
    bool valid;
    // double: holds every uint32_t value of the inverter exactly, which float does not above 2^24
    double Temperature;
    double Energy_Today;
    double DC1_Voltage;
    double DC2_Voltage;
    double DC1_Current;
    double DC2_Current;
    double AC_Current;
    double AC_Voltage;
    double Frequency;
    double Power;
    double Energy_Total;
    double Runtime_Total;
    uint8_t Status;
    uint32_t ErrorBits;
} Solax_LiveData_t;

typedef enum
{
    MQTT_CONNECTED,     // the broker accepted the connection
    MQTT_RETRY,         // broker not reachable, no answer, or not ready: worth another attempt
    MQTT_REFUSED,       // the broker refused the connection (login, password...): another attempt gives the same answer
    MQTT_STOPPED        // stop requested by a signal during the attempt
} Mqtt_Attempt_t;

/*** Static Data ******************************************************************************************/

static char*      arg_TTY_Device   = DEFAULT_TTY_DEVICE_NAME;
static uint16_t   arg_TCP_Port     = DEFAULT_TCP_PORT;

// Always copies owned by the program (set in main(), freed by cleanup()), never string literals
static char*      arg_MQTT_Server   = NULL;
static int        arg_MQTT_Port     = DEFAULT_MQTT_PORT;
static char*      arg_MQTT_Login    = NULL;
static char*      arg_MQTT_Password = NULL;


static int        arg_AV_Samples   = DEFAULT_AVERAGE_SAMPLES;
static uint8_t    arg_InverterAddr = DEFAULT_INVERTER_ADDRESS;
static char*      arg_LogFile      = DEFAULT_LOG_FILE;
static logLevel_t arg_LogLevel     = DEFAULT_LOG_LEVEL;
static int        arg_TestMode     = DEFAULT_TEST_MODE;

static int        fd_tty           = -1;    /* File descriptor for serial interface */
static int        fd_sock_server   = -1;    /* File descriptor for network socket */
static FILE*      fp_log_file      = NULL;  /* File pointer for Log-File */

static uint8_t           solax_InverterSerialNumber[15] = "";
static bool              solax_InverterOnline = false;
static float             solax_QualityOfService = 0;
static Solax_LiveData_t  solax_LiveData = {0};

struct mosquitto * mosq;
static int  mqtt_connack   = -1;     // Answer of the broker to the last connection request: -1 = none yet, 0 = accepted, > 0 = refused
static bool mqtt_connected = false;  // Set by on_connect() and on_disconnect()

// Set to 0 by the signal handler: a volatile sig_atomic_t, an integer type, not a bool
volatile sig_atomic_t run = 1;
// Number of the stop signal received (SIGINT or SIGTERM), written by the signal handler for the stop trace, 0 = none
volatile sig_atomic_t stop_signal = 0;
// Log file descriptor read by the signal handler, -1 = no trace: a lock-free atomic, so that the handler may read it
atomic_int fd_stop_log = -1;
_Static_assert(ATOMIC_INT_LOCK_FREE == 2, "fd_stop_log must be lock-free to be read by the signal handler");

/*** Functions ******************************************************************************************/

// Handler of the stop signals SIGINT (Ctrl+C) and SIGTERM (systemctl stop), and of SIGALRM for the forced exit
void handle_stop(int sig) {
    int saved_errno = errno;
    if (sig == SIGALRM) {
        // Only async-signal-safe functions such as write() may be called here, not stdio or localtime(): the trace has no timestamp
        static const char msg[] = "[NOTE ] Main: Stop requested by signal, forced exit after timeout\n";
        int fd = fd_stop_log;
        if (fd != -1) {
            ssize_t written = write(fd, msg, sizeof(msg) - 1);
            (void)written;  // Nothing to do if the trace fails: the program exits just below
        }
        _exit(EXIT_SUCCESS); // Timeout elapsed while the program is still blocked in a call
    }
    stop_signal = sig;
    run = 0;
    alarm(2); // 2 seconds timeout to exit through the normal path
    errno = saved_errno;
}

void getDateTime(char dateTimeStr[])
{
    struct timespec timeNow;
    struct tm* infoTimeNow;
    long miliSec;  // Type of tv_nsec

    clock_gettime(CLOCK_REALTIME, &timeNow);
    infoTimeNow = localtime(&timeNow.tv_sec);
    miliSec = timeNow.tv_nsec / 1000000;  // Integer division: 0 to 999

    // build time string: "yyyy-MM-DD hh:mm:ss.sss\0"   (len = 24)
    strftime(dateTimeStr, 20, "%Y-%m-%d %H:%M:%S", infoTimeNow);
    sprintf(&dateTimeStr[19], ".%03ld", miliSec);
}



void log_Message(int level, FILE* fp, const char fmt[], ...)
{
    va_list args;
    char dateTime[24];
    
    static const char *level_names[] = {"ERROR", "NOTE ", "INFO ", "DEBUG", "TRACE"};
    #ifdef LOG_USE_COLOR
    static const char *level_colors[] = {"\x1b[31m", "\x1b[32m", "\x1b[33m", "\x1b[36m", "\x1b[94m"};
    #endif
    
    if (level > 4) return;
    
    if ((level == LOG_ERROR) && (fp != stderr))
    {
        va_start(args, fmt);
        vfprintf(stderr, fmt, args);
        va_end(args);
        fprintf(stderr, "\n");
    }
    
    getDateTime(dateTime);
    #ifdef LOG_USE_COLOR
    fprintf(fp, "%s %s[%s]\x1b[0m ", dateTime, level_colors[level], level_names[level]);
    #else
    fprintf(fp, "%s [%s] ", dateTime, level_names[level]);
    #endif
    va_start(args, fmt);
    vfprintf(fp, fmt, args);
    va_end(args);
    fprintf(fp, "\n");
    
    fflush(fp);
}



int log_Bin2Hex(char buff[], const uint8_t data[], const size_t dataLen)
{
    unsigned int x;
    size_t i;
    int len = 0;
    
    if (dataLen)
    {
        for (i=0; i < dataLen; i++)
        {
            if ((i % 8) == 0)
            {
                len += sprintf(&buff[len], " ");
            }
            x = data[i];
            len += sprintf(&buff[len], "%02X ", x);
        }
    }
    else
    {
        len = sprintf(buff, " No Data");
    }
    return len;
}


uint16_t solax_CalculateCRC(const uint8_t data[], const uint8_t dataLen)
{
    uint8_t i;
    uint16_t chkSum = 0;
    
    for (i = 0; i <= dataLen; i++)
    {
        chkSum = chkSum + data[i];
    }
    return chkSum;
}


Solax_ErrorQuery_t solax_RS485_Send(Solax_Message_t* txMessage)
{
    ssize_t txLen;  // Type returned by write()
    uint8_t msgLen;
    uint16_t crc;
    
    txMessage->Header[0] = 0xAA;
    txMessage->Header[1] = 0x55;
    
    msgLen = txMessage->DataLength + 9;
    crc = solax_CalculateCRC((const uint8_t*)txMessage, msgLen - 1);    // calculate out crc bytes
    
    txMessage->Data[txMessage->DataLength + 0] = highByte(crc);
    txMessage->Data[txMessage->DataLength + 1] =  lowByte(crc);
    msgLen += 2;
    
    txLen = write(fd_tty, txMessage, msgLen);
    //tcdrain(fd_tty);    /* delay for output */   // ??????????
    
    if (txLen != msgLen)
    {
        ERROR_MESSAGE("ComTx: Error transmitting data: %s", strerror(errno));
        return ERR_FATAL;
    }
    
    if (arg_LogLevel >= LOG_TRACE)
    {
        char buff[(3 * sizeof(Solax_Message_t)) + (sizeof(Solax_Message_t) / 8) + 3];
        log_Bin2Hex(buff, (const uint8_t*)txMessage, msgLen);
        TRACE_MESSAGE("ComTx:%s", buff);
    }
    
    return ERR_NONE;
}


Solax_ErrorQuery_t solax_RS485_Receive(Solax_Message_t* rxMessage)
{
    ssize_t rxLen; // read length, type returned by read()
    uint8_t msgLen;
    uint16_t crc;
    
    rxLen = read(fd_tty, rxMessage, sizeof(Solax_Message_t));
    
    // Test-Mode: Simulation of inverter data
    if (arg_TestMode)
    {
        static int x = 0;
        static const uint8_t rx_msg_1[] = {0xAA, 0x55, 0x00, 0xFF, 0x01, 0x00, 0x10, 0x80, 0x0E, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31, 0x05, 0x75};
        static const uint8_t rx_msg_2[] = {0xAA, 0x55, 0x00, 0x0A, 0x00, 0x00, 0x10, 0x81, 0x01, 0x06, 0x01, 0xA1};
        static const uint8_t rx_msg_3[] = {0xAA, 0x55, 0x00, 0x0A, 0x01, 0x00, 0x11, 0x82, 0x32, 0x00, 0x0B, 0x00, 0x01, 0x06, 0xDD, 0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00, 0x15, 0x09, 0x21, 0x13, 0x87, 0x01, 0xE7, 0xFF, 0xFF, 0x00, 0x00, 0x12, 0xD3, 0x00, 0x00, 0x0A, 0x0F, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x9C};
        static const uint8_t rx_msg_4[] = {0xAA, 0x55, 0x00, 0x0A, 0x01, 0x00, 0x11, 0x82, 0x32, 0x00, 0x0B, 0x00, 0x01, 0x06, 0xCB, 0x00, 0x00, 0x00, 0x1E, 0x00, 0x00, 0x00, 0x14, 0x09, 0x22, 0x13, 0x89, 0x01, 0xD7, 0xFF, 0xFF, 0x00, 0x00, 0x12, 0xD3, 0x00, 0x00, 0x0A, 0x0F, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x7B};
        if (x == 1) {rxLen = sizeof(rx_msg_1); memcpy(rxMessage, rx_msg_1, sizeof(rx_msg_1));}
        if (x == 2) {rxLen = sizeof(rx_msg_2); memcpy(rxMessage, rx_msg_2, sizeof(rx_msg_2));}
        if (x == 3) {rxLen = sizeof(rx_msg_3); memcpy(rxMessage, rx_msg_3, sizeof(rx_msg_3));}
        if (x == 4) {rxLen = sizeof(rx_msg_4); memcpy(rxMessage, rx_msg_4, sizeof(rx_msg_4));}
        x++; if (x > 4) {x = 3;}
    }
    
    if (rxLen < 0)
    {
        ERROR_MESSAGE("ComRx: Error receiving data: %s", strerror(errno));
        return ERR_FATAL;
    }
    
    if (arg_LogLevel >= LOG_TRACE)
    {
        char buff[(3 * sizeof(Solax_Message_t)) + (sizeof(Solax_Message_t) / 8) + 3];
        log_Bin2Hex(buff, (const uint8_t*)rxMessage, (size_t)rxLen);  // rxLen >= 0: the error case returned above
        TRACE_MESSAGE("ComRx:%s", buff);
    }
    
    if (rxLen == 0)
    {
        return ERR_NO_DATA;
    }
    
    if ((rxLen < 11) || (rxMessage->Header[0] != 0xAA) || (rxMessage->Header[1] != 0x55))
    {
        TRACE_MESSAGE("ComRx: Header fail");
        return ERR_INVALID_MSG;
    }
    
    msgLen = rxMessage->DataLength + 9;
    
    if ((msgLen + 2) > rxLen)
    {
        TRACE_MESSAGE("ComRx: Length fail");
        return ERR_INVALID_MSG;
    }
    
    crc = solax_CalculateCRC((const uint8_t*)rxMessage, msgLen - 1);  // calculate out crc bytes
    
    // check calc crc with received crc
    if (rxMessage->Data[rxMessage->DataLength + 0] != highByte(crc) ||
        rxMessage->Data[rxMessage->DataLength + 1] != lowByte(crc))
    {
        return ERR_CRC_ERROR;
    }

    return ERR_NONE;
}


Solax_ErrorQuery_t solax_Send_Broardcast()
{
    static Solax_Message_t txMessage;
    
    txMessage.Source[0] = 0x01;
    txMessage.Source[1] = 0x00;
    txMessage.Destination[0] = 0x00;
    txMessage.Destination[1] = 0x00;
    txMessage.ControlCode = 0x10;
    txMessage.FunctionCode = 0x00;
    txMessage.DataLength = 0x00;
    
    return solax_RS485_Send(&txMessage);
}


Solax_ErrorQuery_t solax_Send_InverterAddress()
{
    static Solax_Message_t txMessage;
    
    txMessage.Source[0] = 0x00;
    txMessage.Source[1] = 0x00;
    txMessage.Destination[0] = 0x00;
    txMessage.Destination[1] = 0x00;
    txMessage.ControlCode = 0x10;
    txMessage.FunctionCode = 0x01;
    txMessage.DataLength = 0x0F;
    memcpy(txMessage.Data, solax_InverterSerialNumber, 14);
    txMessage.Data[14] = arg_InverterAddr;
    
    return solax_RS485_Send(&txMessage);
}


Solax_ErrorQuery_t solax_Send_QueryLiveData()
{
    static Solax_Message_t txMessage;
    
    txMessage.Source[0] = 0x01;
    txMessage.Source[1] = 0x00;
    txMessage.Destination[0] = 0x00;
    txMessage.Destination[1] = arg_InverterAddr;
    txMessage.ControlCode = 0x11;
    txMessage.FunctionCode = 0x02;
    txMessage.DataLength = 0x00;
    
    return solax_RS485_Send(&txMessage);
}


Solax_ErrorQuery_t solax_SendQuery(const Solax_StateQuery_t stateQuery)
{
    Solax_ErrorQuery_t error;
    
    switch (stateQuery)
    {
        case STATE_BROARDCAST:
        {
            error = solax_Send_Broardcast();
            break;
        }
        
        case STATE_INVERTER_ADDRESS:
        {
            error = solax_Send_InverterAddress();
            break;
        }
        
        case STATE_QUERY_LIVE_DATA:
        {
            error = solax_Send_QueryLiveData();
            break;
        }

        default:
        {
            ERROR_MESSAGE("Solax: Invalid query state %d", stateQuery);
            error = ERR_FATAL;
            break;
        }
    }
    return error;
}


// Publishes an integer value of the inverter (retained message) as a number with the given count of decimals (0 to 2).
// The text is built from the integer itself, without going through a float: 2406 with 1 decimal gives "240.6".
void publish_scaled(const char* topic, uint32_t raw, unsigned int decimals)
{
    static const uint32_t divisors[] = {1, 10, 100};
    char buff[16];  // Large enough for "4294967295" and a decimal point
    int len;

    if (decimals == 0)
        len = snprintf(buff, sizeof(buff), "%" PRIu32, raw);
    else
        len = snprintf(buff, sizeof(buff), "%" PRIu32 ".%0*" PRIu32, raw / divisors[decimals], (int)decimals, raw % divisors[decimals]);

    mosquitto_publish(mosq, NULL, topic, len, buff, 0, true);
}


Solax_ErrorQuery_t solax_ReceiveQuery(const Solax_StateQuery_t stateQuery, Solax_LiveData_t* liveData)
{
    static Solax_Message_t rxMessage;

    uint32_t value;
    Solax_ErrorQuery_t error;
        
    error = solax_RS485_Receive(&rxMessage);
    if (error == ERR_FATAL) return ERR_FATAL;
    
    *liveData = (Solax_LiveData_t) {0};
        
    switch (stateQuery)
    {
        case STATE_BROARDCAST:
        {
            if (error == ERR_NO_DATA)
            {
                // Maybe the Solax-X1 inverter is offline (e.g. no sunlight);
            DEBUG_MESSAGE("Solax: No broadcast response");
            }
            else if (error == ERR_CRC_ERROR)
            {
                DEBUG_MESSAGE("Solax: Broadcast response CRC error");
            }
            else if ((error == ERR_INVALID_MSG) || (rxMessage.ControlCode != 0x10) || (rxMessage.FunctionCode != 0x80))
            {
                DEBUG_MESSAGE("Solax: Invalid broadcast response message");
            }
            else
            {
                // Serial number from query response
                memcpy(solax_InverterSerialNumber, rxMessage.Data, 14);
                solax_InverterSerialNumber[14] = '\0';
                DEBUG_MESSAGE("Solax: Serial number: %s", solax_InverterSerialNumber);
            }
            break;
        }
        
        case STATE_INVERTER_ADDRESS:
        {
            if (error == ERR_NO_DATA)
            {
                DEBUG_MESSAGE("Solax: No address confirmation response");
            }
            else if (error == ERR_CRC_ERROR)
            {
                DEBUG_MESSAGE("Solax: Address confirmation response CRC error");
            }
            else if ((error == ERR_INVALID_MSG) || (rxMessage.ControlCode != 0x10) || (rxMessage.FunctionCode != 0x81) || (rxMessage.Data[0] != 0x06))
            {
                DEBUG_MESSAGE("Solax: Invalid address confirmation message");
            }
            else
            {
                DEBUG_MESSAGE("Solax: Inverter Bus-Address 0x%02X confirmed", arg_InverterAddr);
            }
            break;
        }
        
        case STATE_QUERY_LIVE_DATA:
        {
            if (error == ERR_NO_DATA)
            {
                DEBUG_MESSAGE("Solax: No live data response");
            }
            else if (error == ERR_CRC_ERROR)
            {
                DEBUG_MESSAGE("Solax: Data response CRC error");
            }
            else if ((error == ERR_INVALID_MSG) || (rxMessage.ControlCode != 0x11) || (rxMessage.FunctionCode != 0x82))
            {
                DEBUG_MESSAGE("Solax: Invalid live data message");
            }
            else
            {
                value = (rxMessage.Data[0] << 8) | rxMessage.Data[1]; // Temperature [�C]
                liveData->Temperature = value;
                DEBUG_MESSAGE("Solax: LiveData.Temperature: %.0f C", liveData->Temperature);
                publish_scaled("solax/Temperature", value, 0);
                
                value = (rxMessage.Data[2] << 8) | rxMessage.Data[3]; // Energy Today [kWh]
                liveData->Energy_Today = value * 0.1;
                DEBUG_MESSAGE("Solax: LiveData.Energy_Today: %.1f kWh", liveData->Energy_Today);
                publish_scaled("solax/Energy_Today", value, 1);
                
                value = (rxMessage.Data[4] << 8) | rxMessage.Data[5]; // PV1 Voltage [V]
                liveData->DC1_Voltage = value * 0.1;
                DEBUG_MESSAGE("Solax: LiveData.DC1_Voltage: %.1f V", liveData->DC1_Voltage);
                publish_scaled("solax/DC1_Voltage", value, 1);
                
                value = (rxMessage.Data[6] << 8) | rxMessage.Data[7]; // PV2 Voltage [V]
                liveData->DC2_Voltage = value * 0.1;
                DEBUG_MESSAGE("Solax: LiveData.DC2_Voltage: %.1f V", liveData->DC2_Voltage);
                publish_scaled("solax/DC2_Voltage", value, 1);
                
                value = (rxMessage.Data[8] << 8) | rxMessage.Data[9]; // PV1 Current [A]
                liveData->DC1_Current = value * 0.1;
                DEBUG_MESSAGE("Solax: LiveData.DC1_Current: %.1f A", liveData->DC1_Current);
                publish_scaled("solax/DC1_Current", value, 1);
                
                value = (rxMessage.Data[10] << 8) | rxMessage.Data[11]; // PV2 Current [A]
                liveData->DC2_Current = value * 0.1;
                DEBUG_MESSAGE("Solax: LiveData.DC2_Current: %.1f A", liveData->DC2_Current);
                publish_scaled("solax/DC2_Current", value, 1);
                
                value = (rxMessage.Data[12] << 8) | rxMessage.Data[13]; // AC Current [A]
                liveData->AC_Current = value * 0.1;
                DEBUG_MESSAGE("Solax: LiveData.AC_Current: %.1f A", liveData->AC_Current);
                publish_scaled("solax/AC_Current", value, 1);
                
                value = (rxMessage.Data[14] << 8) | rxMessage.Data[15]; // AC Voltage [V]
                liveData->AC_Voltage = value * 0.1;
                DEBUG_MESSAGE("Solax: LiveData.AC_Voltage: %.1f V", liveData->AC_Voltage);
                publish_scaled("solax/AC_Voltage", value, 1);
                
                value = (rxMessage.Data[16] << 8) | rxMessage.Data[17]; // AC Frequency [Hz]
                liveData->Frequency = value * 0.01;
                DEBUG_MESSAGE("Solax: LiveData.Frequency: %.2f Hz", liveData->Frequency);
                publish_scaled("solax/Frequency", value, 2);
                
                value = (rxMessage.Data[18] << 8) | rxMessage.Data[19]; // AC Power [W]
                liveData->Power = value;
                DEBUG_MESSAGE("Solax: LiveData.Power: %.0f W", liveData->Power);
                publish_scaled("solax/Power", value, 0);
                
                //value = (rxMessage.Data[20] << 8) | rxMessage.Data[21]; // Not Used
                //DEBUG_MESSAGE("Solax: LiveData.Not_Used: %.1f kWh", value);
                //publish_scaled("solax/Not_Used", value, 0);
                
                value = (rxMessage.Data[22] << 24) | (rxMessage.Data[23] << 16) | (rxMessage.Data[24] << 8) | rxMessage.Data[25]; // Energy Total [kWh]
                if (value) {liveData->Energy_Total = value * 0.1;}
                DEBUG_MESSAGE("Solax: LiveData.Energy_Total: %.1f kWh", liveData->Energy_Total);
                publish_scaled("solax/Energy_Total", value, 1);
                
                value = (rxMessage.Data[26] << 24) | (rxMessage.Data[27] << 16) | (rxMessage.Data[28] << 8) | rxMessage.Data[29]; // Work Time Total [hour]
                if (value) {liveData->Runtime_Total = value;}
                DEBUG_MESSAGE("Solax: LiveData.Runtime_Total: %.0f h", liveData->Runtime_Total);
                publish_scaled("solax/Runtime_Total", value, 0);
                
                value = (rxMessage.Data[30] << 8) | rxMessage.Data[31]; // Work mode [???]
                liveData->Status = (uint8_t)value;
                DEBUG_MESSAGE("Solax: LiveData.Status: %d", liveData->Status);
                publish_scaled("solax/Status", liveData->Status, 0);
                
                //value = (rxMessage.Data[32] << 8) | rxMessage.Data[33]; // Grid voltage fault in 0.1V
                //value = (rxMessage.Data[34] << 8) | rxMessage.Data[35]; // Gird frequency fault in 0.01Hz
                //value = (rxMessage.Data[36] << 8) | rxMessage.Data[37]; // DC injection fault in 1mA
                //value = (rxMessage.Data[38] << 8) | rxMessage.Data[39]; // Temperature fault in �C
                //value = (rxMessage.Data[40] << 8) | rxMessage.Data[41]; // Pv1 voltage fault in 0.1V
                //value = (rxMessage.Data[42] << 8) | rxMessage.Data[43]; // Pv2 voltage fault in 0.1V
                //value = (rxMessage.Data[44] << 8) | rxMessage.Data[45]; // GFC fault
                
                value = (rxMessage.Data[49] << 24) | (rxMessage.Data[48] << 16) | (rxMessage.Data[47] << 8) | rxMessage.Data[46]; // Error Code
                liveData->ErrorBits = (uint32_t)value;
                DEBUG_MESSAGE("Solax: LiveData.ErrorBits: 0x%08X", liveData->ErrorBits);
                
                liveData->valid = true;
            }
            break;
        }
    }
    return error;
}


/* --- State Machine Communication with Solax-X1_Mini --- */
int solax_QueryHandle(Solax_LiveData_t* liveData)
{
    static Solax_StateQuery_t stateQuery = STATE_QUERY_LIVE_DATA;
    static int timeoutInverterOnline = TIMEOUT_INVERTER_ONLINE;
    static int countError = 0;
    
    Solax_ErrorQuery_t errorRx;
    Solax_ErrorQuery_t errorTx;
        
    errorRx = solax_ReceiveQuery(stateQuery, liveData);
    if (errorRx == ERR_FATAL) return -1;
    
    switch (stateQuery)
    {
        case STATE_BROARDCAST:
        {
            if (errorRx)
            {
                countError++;
                if (countError >= 10)
                {
                    countError = 0;
                    stateQuery = STATE_QUERY_LIVE_DATA;
                }
            }
            else
            {
                countError = 0;
                stateQuery = STATE_INVERTER_ADDRESS;
            }
            break;
        }
        
        case STATE_INVERTER_ADDRESS:
        {
            if (errorRx)
            {
                countError++;
                if (countError >= 3)
                {
                    countError = 0;
                    stateQuery = STATE_BROARDCAST;
                }
            }
            else
            {
                countError = 0;
                stateQuery = STATE_QUERY_LIVE_DATA;
            }
            break;
        }
        
        case STATE_QUERY_LIVE_DATA:
        {
            if (errorRx)
            {
                countError++;
                if (countError >= 3)
                {
                    countError = 0;
                    stateQuery = STATE_BROARDCAST;
                }
            }
            else
            {
                timeoutInverterOnline = 0;
            }
            break;
        }
    }
    
    if (solax_InverterOnline == true)
    {
        timeoutInverterOnline++;
        if (timeoutInverterOnline >= TIMEOUT_INVERTER_ONLINE)
        {
            solax_InverterOnline = false;
            NOTICE_MESSAGE("Solax: Inverter offline");
        }
    }
    else  // (solax_InverterOnline == false)
    {
        if (timeoutInverterOnline == 0)
        {
            solax_InverterOnline = true;
            NOTICE_MESSAGE("Solax: Live data received");
        }
    }

    
    errorTx = solax_SendQuery(stateQuery);
    if (errorTx == ERR_FATAL) return -1;
    
    return errorRx;
}



void solax_LiveData_Average(const Solax_LiveData_t* liveData, uint8_t idx, uint8_t max_idx)
{
    uint8_t i, cnt = 0, qos = 0;

    solax_LiveData = (Solax_LiveData_t) {0};

    for (i = 0; i <= arg_AV_Samples; i++)
    {
        if (liveData[idx].valid != true) continue;
        solax_LiveData.Temperature  += liveData[idx].Temperature;
        solax_LiveData.DC1_Voltage  += liveData[idx].DC1_Voltage;
        solax_LiveData.DC2_Voltage  += liveData[idx].DC2_Voltage;
        solax_LiveData.DC1_Current  += liveData[idx].DC1_Current;
        solax_LiveData.DC2_Current  += liveData[idx].DC2_Current;
        solax_LiveData.AC_Current   += liveData[idx].AC_Current;
        solax_LiveData.AC_Voltage   += liveData[idx].AC_Voltage;
        solax_LiveData.Frequency    += liveData[idx].Frequency;
        solax_LiveData.Power        += liveData[idx].Power;
        solax_LiveData.ErrorBits    |= liveData[idx].ErrorBits;
        if (solax_LiveData.Energy_Today  < liveData[idx].Energy_Today ) solax_LiveData.Energy_Today  = liveData[idx].Energy_Today;
        if (solax_LiveData.Energy_Total  < liveData[idx].Energy_Total ) solax_LiveData.Energy_Total  = liveData[idx].Energy_Total;
        if (solax_LiveData.Runtime_Total < liveData[idx].Runtime_Total) solax_LiveData.Runtime_Total = liveData[idx].Runtime_Total;
        if (solax_LiveData.Status        < liveData[idx].Status       ) solax_LiveData.Status        = liveData[idx].Status;
        
        if (idx)
        {
            idx--;
        }
        else
        {
            idx = max_idx;
        }
        cnt++;
    }
    
    if (cnt)
    {
        solax_LiveData.Temperature /= cnt;
        solax_LiveData.DC1_Voltage /= cnt;
        solax_LiveData.DC2_Voltage /= cnt;
        solax_LiveData.DC1_Current /= cnt;
        solax_LiveData.DC2_Current /= cnt;
        solax_LiveData.AC_Current  /= cnt;
        solax_LiveData.AC_Voltage  /= cnt;
        solax_LiveData.Frequency   /= cnt;
        solax_LiveData.Power       /= cnt;
    }
    

    for (idx = 0; idx <= max_idx; idx++)
    {
        qos += liveData[idx].valid;
    }
    
    solax_QualityOfService = (float)qos / (max_idx + 1);
    
    /*
    INFO_MESSAGE("TEST: solax_LiveData.QualityOfService: %.2f", solax_QualityOfService);
    INFO_MESSAGE("TEST: solax_LiveData.Temperature: %.0f C",    solax_LiveData.Temperature);
    INFO_MESSAGE("TEST: solax_LiveData.DC1_Voltage: %.1f V",    solax_LiveData.DC1_Voltage);
    INFO_MESSAGE("TEST: solax_LiveData.DC2_Voltage: %.1f V",    solax_LiveData.DC2_Voltage);
    INFO_MESSAGE("TEST: solax_LiveData.DC1_Current: %.1f A",    solax_LiveData.DC1_Current);
    INFO_MESSAGE("TEST: solax_LiveData.DC2_Current: %.1f A",    solax_LiveData.DC2_Current);
    INFO_MESSAGE("TEST: solax_LiveData.AC_Current: %.1f A",     solax_LiveData.AC_Current);
    INFO_MESSAGE("TEST: solax_LiveData.AC_Voltage: %.1f V",     solax_LiveData.AC_Voltage);
    INFO_MESSAGE("TEST: solax_LiveData.Frequency: %.2f Hz",     solax_LiveData.Frequency);
    INFO_MESSAGE("TEST: solax_LiveData.Power: %.0f W",          solax_LiveData.Power);
    INFO_MESSAGE("TEST: solax_LiveData.Energy_Today: %.1f kWh", solax_LiveData.Energy_Today);
    INFO_MESSAGE("TEST: solax_LiveData.Energy_Total: %.1f kWh", solax_LiveData.Energy_Total);
    INFO_MESSAGE("TEST: solax_LiveData.Runtime_Total: %.0f h",  solax_LiveData.Runtime_Total);
    INFO_MESSAGE("TEST: solax_LiveData.Status: %d",             solax_LiveData.Status);
    INFO_MESSAGE("TEST: solax_LiveData.ErrorBits: 0x%08X",      solax_LiveData.ErrorBits);
    INFO_MESSAGE("TEST: Counter: %d", cnt);
    */
}


int solax_JsonPath(char buffer[], Solax_LiveData_t* liveData)
{
    // Meaning of the 32 bits of error_bits. Not used yet: kept to publish the error texts.
    static const char* solax_ErrorText[32] __attribute__((unused)) =
    {
        "Tz Protection Fault",           // Byte 0.0
        "Mains Lost Fault",              // Byte 0.1
        "Grid Voltage Fault",            // Byte 0.2
        "Grid Frequency Fault",          // Byte 0.3
        "PLL Lost Fault",                // Byte 0.4
        "Bus Voltage Fault",             // Byte 0.5
        "Error Bit 06",                  // Byte 0.6
        "Oscillator Fault",              // Byte 0.7
        
        "DCI OCP Fault",                 // Byte 1.0
        "Residual Current Fault",        // Byte 1.1
        "PV Voltage Fault",              // Byte 1.2
        "Ac10Mins Voltage Fault",        // Byte 1.3
        "Isolation Fault",               // Byte 1.4
        "Over Temperature Fault",        // Byte 1.5
        "Ventilator Fault",              // Byte 1.6
        "Error Bit 15",                  // Byte 1.7
        
        "SPI Communication Fault",       // Byte 2.0
        "SCI Communication Fault",       // Byte 2.1
        "Error Bit 18",                  // Byte 2.2
        "Input Configuration Fault",     // Byte 2.3
        "EEPROM Fault",                  // Byte 2.4
        "Relay Fault",                   // Byte 2.5
        "Sample Consistence Fault",      // Byte 2.6
        "Residual-Current Device Fault", // Byte 2.7
        
        "Error Bit 24",                  // Byte 3.0
        "Error Bit 25",                  // Byte 3.1
        "Error Bit 26",                  // Byte 3.2
        "Error Bit 27",                  // Byte 3.3
        "Error Bit 28",                  // Byte 3.4
        "DCI Device Fault",              // Byte 3.5
        "Other Device Fault",            // Byte 3.6
        "Error Bit 31",                  // Byte 3.7
    };
    
    INFO_MESSAGE("JsonP: Inverter.Address: %d",            arg_InverterAddr);
    INFO_MESSAGE("JsonP: Inverter.Online: %d",             solax_InverterOnline);
    INFO_MESSAGE("JsonP: Inverter.QualityOfService: %.2f", solax_QualityOfService);
    INFO_MESSAGE("JsonP: LiveData.DC1_Voltage: %.1f V",    liveData->DC1_Voltage);
    INFO_MESSAGE("JsonP: LiveData.DC2_Voltage: %.1f V",    liveData->DC2_Voltage);
    INFO_MESSAGE("JsonP: LiveData.DC1_Current: %.1f A",    liveData->DC1_Current);
    INFO_MESSAGE("JsonP: LiveData.DC2_Current: %.1f A",    liveData->DC2_Current);
    INFO_MESSAGE("JsonP: LiveData.AC_Current: %.1f A",     liveData->AC_Current);
    INFO_MESSAGE("JsonP: LiveData.AC_Voltage: %.1f V",     liveData->AC_Voltage);
    INFO_MESSAGE("JsonP: LiveData.Frequency: %.2f Hz",     liveData->Frequency);
    INFO_MESSAGE("JsonP: LiveData.Power: %.0f W",          liveData->Power);
    INFO_MESSAGE("JsonP: LiveData.Energy_Today: %.1f kWh", liveData->Energy_Today);
    INFO_MESSAGE("JsonP: LiveData.Energy_Total: %.1f kWh", liveData->Energy_Total);
    INFO_MESSAGE("JsonP: LiveData.Runtime_Total: %.0f h",  liveData->Runtime_Total);
    INFO_MESSAGE("JsonP: LiveData.Status: %d",             liveData->Status);
    INFO_MESSAGE("JsonP: LiveData.ErrorBits: 0x%08X",      liveData->ErrorBits);
    
    int len = 0;
    len += sprintf(&buffer[len], "{\r\n");                                                          //
    len += sprintf(&buffer[len], "  \"inverter\":\r\n");                                            //
    len += sprintf(&buffer[len], "  {\r\n");                                                        //
    len += sprintf(&buffer[len], "    \"address\": %d,\r\n",              arg_InverterAddr);        //
    len += sprintf(&buffer[len], "    \"online\": %d,\r\n",               solax_InverterOnline);    //
    len += sprintf(&buffer[len], "    \"quality_of_service\": %.2f,\r\n", solax_QualityOfService);  //
    len += sprintf(&buffer[len], "    \"live_data\":\r\n");                                         //
    len += sprintf(&buffer[len], "    {\r\n");                                                      //
    len += sprintf(&buffer[len], "      \"temperature\": %.0f,\r\n",      liveData->Temperature);   //
    len += sprintf(&buffer[len], "      \"dc1_voltage\": %.1f,\r\n",      liveData->DC1_Voltage);   //
    len += sprintf(&buffer[len], "      \"dc1_current\": %.1f,\r\n",      liveData->DC1_Current);   //
    len += sprintf(&buffer[len], "      \"dc2_voltage\": %.1f,\r\n",      liveData->DC2_Voltage);   //
    len += sprintf(&buffer[len], "      \"dc2_current\": %.1f,\r\n",      liveData->DC2_Current);   //
    len += sprintf(&buffer[len], "      \"ac_voltage\": %.1f,\r\n",       liveData->AC_Voltage);    //
    len += sprintf(&buffer[len], "      \"ac_current\": %.1f,\r\n",       liveData->AC_Current);    //
    len += sprintf(&buffer[len], "      \"frequency\": %.2f,\r\n",        liveData->Frequency);     //
    len += sprintf(&buffer[len], "      \"power\": %.0f,\r\n",            liveData->Power);         //
    len += sprintf(&buffer[len], "      \"energy_today\": %.1f,\r\n",     liveData->Energy_Today);  //
    len += sprintf(&buffer[len], "      \"energy_total\": %.1f,\r\n",     liveData->Energy_Total);  //
    len += sprintf(&buffer[len], "      \"runtime_total\": %.0f,\r\n",    liveData->Runtime_Total); //
    len += sprintf(&buffer[len], "      \"status\": %d,\r\n",             liveData->Status);        //
    len += sprintf(&buffer[len], "      \"error_bits\": %u\r\n",          liveData->ErrorBits);     //
    len += sprintf(&buffer[len], "    }\r\n");                                                      //
    len += sprintf(&buffer[len], "  }\r\n");                                                        //
    len += sprintf(&buffer[len], "}\r\n");                                                          //
    
    return len;
}



int poll_HTTP_Server(void)
{
    int fd_sock_client;
    char response[1000];
    int len = 0;

    fd_sock_client = accept(fd_sock_server, NULL, NULL);
 
    if ((fd_sock_client == -1) && (errno != EWOULDBLOCK))
    {
        ERROR_MESSAGE("Http: Error when accepting HTTP-Client connection: %s", strerror(errno));
        return -1;
    }
    
    if (fd_sock_client >= 0)  
    {
        DEBUG_MESSAGE("HTTP: Got a connection");
                
        /*
        len += sprintf(&response[len], "HTTP/1.0 200 OK\r\n");
        len += sprintf(&response[len], "Connection: close\r\n");
        len += sprintf(&response[len], "Content-Type: text/html; charset=utf-8\r\n");
        len += sprintf(&response[len], "\r\n");
        len += sprintf(&response[len], "<html><head><title>SolaXd</title></head>\r\n");
        len += sprintf(&response[len], "<body>\r\n");
        len += solax_JsonPath(&response[len], &solax_LiveData);
        len += sprintf(&response[len], "</body></html>\r\n");
        */
        
        len += sprintf(&response[len], "HTTP/1.0 200 OK\r\n");
        len += sprintf(&response[len], "Connection: close\r\n");
        len += sprintf(&response[len], "Content-Type: application/json\r\n");
        len += sprintf(&response[len], "\r\n");
        len += solax_JsonPath(&response[len], &solax_LiveData);

        ssize_t written = write(fd_sock_client, response, (size_t)len);  // len >= 0: sum of the sprintf() results
        if (written < 0)
        {
            ERROR_MESSAGE("Http: Error sending the response: %s", strerror(errno));
        }
        else if (written != len)
        {
            ERROR_MESSAGE("Http: Response truncated: %zd of %d bytes sent", written, len);
        }
        close(fd_sock_client);
    }
    return 0;
}


int init_HTTP_Server(uint16_t port)
{
    int error;
    int flags;
    int enable = 1;
    struct sockaddr_in addr_server;

    fd_sock_server = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_sock_server == -1) { ERROR_MESSAGE("Init: Error opening socket for HTTP-Server at port '%d': %s", port, strerror(errno)); return -1; }
    
    flags = fcntl(fd_sock_server, F_GETFL);
    flags = fcntl(fd_sock_server, F_SETFL, flags | O_NONBLOCK);
    
    setsockopt(fd_sock_server, SOL_SOCKET, SO_REUSEADDR, &enable , sizeof(int));
    
    // filling server information
    addr_server.sin_family = AF_INET;
    addr_server.sin_addr.s_addr = INADDR_ANY;
    addr_server.sin_port = htons(port);

    error = bind(fd_sock_server, (struct sockaddr *) &addr_server, sizeof(addr_server));
    if (error == -1) { ERROR_MESSAGE("Init: Error binding socket for HTTP-Server at port '%d': %s", port, strerror(errno)); return -1; }

    error = listen(fd_sock_server, 10);
    if (error == -1) { ERROR_MESSAGE("Init: Error listening socket for HTTP-Server at port '%d': %s", port, strerror(errno)); return -1; }
    
    NOTICE_MESSAGE("Init: HTTP-Server at port '%d' created successfully", port);
    return 0;
}


int init_Serial_Interface(char device[])
{
    struct termios tty;
    
    fd_tty = open(device, O_RDWR | O_NOCTTY | O_SYNC);
    if (fd_tty < 0)
    {
        ERROR_MESSAGE("Init: Error opening '%s': %s", device, strerror(errno));
        return -1;
    }
    
    if (tcgetattr(fd_tty, &tty) < 0)
    {
        ERROR_MESSAGE("Init: Error opening '%s': %s", device, strerror(errno));
        return -1;
    }
    
    // Raw mode, 9600 bauds, 8N1: 8 data bits, no parity (no PARENB),
    // 1 stop bit (no CSTOPB), no hardware flow control (no CRTSCTS)
    tty.c_cflag = CLOCAL | CREAD | CS8;  // ignore modem controls, enable the receiver, 8-bit characters
    tty.c_iflag = 0;
    tty.c_oflag = 0;
    tty.c_lflag = 0;

    cfsetospeed(&tty, (speed_t)B9600);  // Only sets the speed bits
    cfsetispeed(&tty, (speed_t)B9600);

    /* completely non-blocking read */
    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 0;
    
    if (tcsetattr(fd_tty, TCSANOW, &tty) != 0)
    {
        ERROR_MESSAGE("Init: Error opening '%s': %s", device, strerror(errno));
        return -1;
    }
    
    NOTICE_MESSAGE("Init: Device '%s' opened successfully", device);
    return 0;
}

void close_mqtt()
{
	mosquitto_disconnect(mosq);
	mosquitto_destroy(mosq);
	mosquitto_lib_cleanup();
	mosq = NULL;  // Lets cleanup() know that there is no session left to close
}

// Single exit path of main(): closes everything opened after the log file, then the log file
void cleanup(void)
{
    if (mosq != NULL) close_mqtt();
    if (fd_sock_server != -1) { close(fd_sock_server); fd_sock_server = -1; }
    if (fd_tty != -1) { close(fd_tty); fd_tty = -1; }
    if (fp_log_file != NULL && fp_log_file != stderr)
    {
        fd_stop_log = -1;  // Before fclose(): the signal handler must not write to a closed or reused descriptor
        fclose(fp_log_file);
        fp_log_file = NULL;
    }
    free(arg_MQTT_Server);   arg_MQTT_Server   = NULL;
    free(arg_MQTT_Login);    arg_MQTT_Login    = NULL;
    free(arg_MQTT_Password); arg_MQTT_Password = NULL;
}

// Called by mosquitto_loop() when the broker answers the connection request (CONNACK): rc = 0 accepted, > 0 refused
void on_connect(struct mosquitto* client, void* userdata, int rc)
{
    (void)client;
    (void)userdata;
    mqtt_connack = rc;
    if (rc == 0) mqtt_connected = true;
}

// Called by mosquitto_loop() when the connection is closed, whatever the reason
void on_disconnect(struct mosquitto* client, void* userdata, int rc)
{
    (void)client;
    (void)userdata;
    (void)rc;
    mqtt_connected = false;
}

// One connection attempt: connection to the broker, then wait for its answer (CONNACK) for at most MQTT_CONNACK_TIMEOUT seconds.
// On MQTT_RETRY and MQTT_REFUSED, *reason describes the failure.
Mqtt_Attempt_t mqtt_connect_once(const char** reason)
{
    int error;

    mqtt_connack = -1;
    error = mosquitto_connect(mosq, arg_MQTT_Server, arg_MQTT_Port, 60);
    if (run == 0) return MQTT_STOPPED;
    if (error != MOSQ_ERR_SUCCESS) {
        *reason = (error == MOSQ_ERR_ERRNO) ? strerror(errno) : mosquitto_strerror(error);
        return MQTT_RETRY;
    }

    // 100 ms steps: mosquitto_loop() returns as soon as the answer has been read
    for (int j = 0; j < MQTT_CONNACK_TIMEOUT * 10 && mqtt_connack == -1 && run != 0; ++j) {
        error = mosquitto_loop(mosq, 100, 1);
        if (error != MOSQ_ERR_SUCCESS) break;  // A refusal is also reported here, after on_connect() has set mqtt_connack
    }
    if (run == 0) return MQTT_STOPPED;
    if (mqtt_connack == 0) return MQTT_CONNECTED;
    if (mqtt_connack > 0) {
        *reason = mosquitto_connack_string(mqtt_connack);
        // 3 = "Server unavailable": the broker is there but not ready, the other refusals need a change of the settings
        return (mqtt_connack == 3) ? MQTT_RETRY : MQTT_REFUSED;
    }
    if (error != MOSQ_ERR_SUCCESS) {
        *reason = (error == MOSQ_ERR_ERRNO) ? strerror(errno) : mosquitto_strerror(error);
    } else {
        *reason = "no answer from the broker";
    }
    return MQTT_RETRY;
}

int init_mqtt()
{
    int error;
    int max_retries = 12; // Maximum number of connection retries
    unsigned int retry_interval = MQTT_RETRY_INTERVAL; // Type expected by sleep()
    const char* reason = "";

    error = mosquitto_lib_init();
    if (error != MOSQ_ERR_SUCCESS) {
        ERROR_MESSAGE("Init MQTT: Error initializing the library: %s", mosquitto_strerror(error));
        return -1;
    }
    mosq = mosquitto_new("solaXdtoMQTT", true, NULL);
    if (mosq == NULL) {
        ERROR_MESSAGE("Init MQTT: Error creating the client: %s", strerror(errno));
        mosquitto_lib_cleanup();  // close_mqtt() is not called when there is no client
        return -1;
    }
    error = mosquitto_username_pw_set(mosq, arg_MQTT_Login, arg_MQTT_Password);
    if (error != MOSQ_ERR_SUCCESS) {
        ERROR_MESSAGE("Init MQTT: Error setting the login and password: %s", mosquitto_strerror(error));
        return -1;
    }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);

    // Connection retries
    for (int i = 0; i < max_retries && run != 0; ++i) {
        Mqtt_Attempt_t attempt = mqtt_connect_once(&reason);
        if (attempt == MQTT_STOPPED) {
            break;  // Stop requested during the connection attempt
        }
        if (attempt == MQTT_CONNECTED) {
            NOTICE_MESSAGE("Init MQTT: Connected, the broker accepted the connection");
            return 0;
        } else if (attempt == MQTT_REFUSED) {
            ERROR_MESSAGE("Init MQTT: The broker refused the connection: %s", reason);
            return -1;
        } else if (i < max_retries - 1) {
            ERROR_MESSAGE("Init MQTT: Broker not reachable: %s", reason);
            NOTICE_MESSAGE("Retrying connection in %u seconds...", retry_interval);
            sleep(retry_interval);
        } else {
            ERROR_MESSAGE("Maximum retries reached: Exiting");
            return -1;
        }
    }

    // Stop requested before the connection was established
    NOTICE_MESSAGE("Init MQTT: Stop requested by signal %d (%s) during connection, exiting", (int)stop_signal, strsignal((int)stop_signal));
    return -1;
}

// Opens the serial port, the MQTT session and the HTTP server, then runs the main loop until a stop or an error.
// Returns the exit code of the program. main() calls cleanup() afterwards, whatever the return path.
int run_daemon(void)
{
    int error;
    uint8_t index = 0;
    Solax_LiveData_t liveData[MAX_INDEX_OF_LIVE_DATA + 1] = {0};

    error = init_Serial_Interface(arg_TTY_Device);  // open COM-Port
    if (error == -1) return EXIT_FAILURE;

    error = init_mqtt();                            // connect to MQTT server
    if (error == -1) return (run == 0) ? EXIT_SUCCESS : EXIT_FAILURE;  // stop requested during the connection: normal stop

    error = init_HTTP_Server(arg_TCP_Port);         // open TCP-Listener
    if (error == -1) return EXIT_FAILURE;

    while (run != 0)
    {
        error = mosquitto_loop(mosq,0,1);  // No wait: sleep(1) alone sets the 1 second period of the loop
        if (error != MOSQ_ERR_SUCCESS) {
            ERROR_MESSAGE("MQTT client connection lost - Error Code: %d", error);
            return EXIT_FAILURE;
        }

        error = solax_QueryHandle(&liveData[index]);
        if (error == -1) return EXIT_FAILURE;

        solax_LiveData_Average(liveData, index, MAX_INDEX_OF_LIVE_DATA);

        index++;
        if (index > MAX_INDEX_OF_LIVE_DATA) index = 0;

        error = poll_HTTP_Server();
        if (error == -1) return EXIT_FAILURE;

        sleep(1);
    }
    NOTICE_MESSAGE("Main: Stop requested by signal %d (%s), exiting", (int)stop_signal, strsignal((int)stop_signal));
    return EXIT_SUCCESS;
}

/*** MAIN ******************************************************************************************/

int main(int argc, char* argv[])
{
    // Without SA_RESTART: blocking calls (connect, sleep) are interrupted by the signal
    struct sigaction sa = {0};
    sa.sa_handler = handle_stop;
    sigemptyset(&sa.sa_mask);
    // SIGPIPE ignored: writing to an HTTP client that has closed the connection fails with EPIPE instead of killing the program
    struct sigaction sa_ignore = {0};
    sa_ignore.sa_handler = SIG_IGN;
    sigemptyset(&sa_ignore.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) == -1 || sigaction(SIGTERM, &sa, NULL) == -1 || sigaction(SIGALRM, &sa, NULL) == -1 ||
        sigaction(SIGPIPE, &sa_ignore, NULL) == -1) {
        perror("Error configuring signal handler");
        return EXIT_FAILURE;
    }

    int opt;
    int value;  // Numeric option checked before being stored in a smaller type
    // Values given on the command line: pointers into argv, copied once after parsing
    const char* opt_server = NULL;
    const char* opt_login = NULL;
    const char* opt_password = NULL;

    if ((argc == 2) && (strcmp(argv[1], "--version") == 0))
    {
        printf("%s, %s\n", SOLARXD_STRING, VERSION_STRING);
        printf("Copyright (C) 2021 Jens Jordan\n");
        printf("License GPLv3+: GNU GPL version 3 or later <http://gnu.org/licenses/gpl.html>.\n");
        printf("There is NO WARRANTY, to the extent permitted by law.\n");
        printf("This is free software; you are free to change and redistribute it.\n");
        return 0;
    }
    
    if ((argc == 2) && (strcmp(argv[1], "--help") == 0))
    {
        printf("Usage: %s [OPTION] ...\n", SOLARXD_STRING);
        printf("Daemon for communication with SolaX-X1_Mini inverter via RS485.\n");
        printf("    Options:      The default value for each option is shown in square brackets.\n");
        printf("      -d <DEV>    Use DEV as SolaXd serial port device  [%s]\n", DEFAULT_TTY_DEVICE_NAME);
        printf("      -p <PORT>   Port of HTTP-Server  [%d]\n", DEFAULT_TCP_PORT);
        printf("      -S <SERVER> MQTT server address  [%s]\n", DEFAULT_MQTT_SERVER);
        printf("      -P <PORT>   Port of MQTT server  [%d]\n", DEFAULT_MQTT_PORT);
        printf("      -U <LOGIN>  MQTT login, overrides $SOLAXD_MQTT_LOGIN  [%s]\n", DEFAULT_MQTT_LOGIN);
        printf("      -W <PASSWD> MQTT password, overrides $SOLAXD_MQTT_PASSWORD  [%s]\n", DEFAULT_MQTT_PASSWORD);
        printf("      -s <SAMPLE> Samples used for average calculation  [%d]\n", DEFAULT_AVERAGE_SAMPLES);
        printf("      -a <ADDR>   Use ADDR as inverter bus address  [%d]\n", DEFAULT_INVERTER_ADDRESS);
        printf("      -l <FILE>   Write log to FILE, instead to stderr\n");
        printf("      -L <LEVEL>  LEVEL: 0=error/1=notice/2=info/3=debug/4=trace  [%d]\n", DEFAULT_LOG_LEVEL);
        printf("      -x          Enable test mode with simulated inverter data\n");
        printf("      --help      Display this help and exit\n");
        printf("      --version   Output version information and exit\n");
        return 0;
    }
    
    while ((opt = getopt (argc, argv, ":d:p:S:P:U:W:s:a:l:L:x")) != -1)
    {
        switch (opt)
        {
            case 'd':
                arg_TTY_Device = optarg;
                break;
            case 'p':
                value = atoi(optarg);
                if (value < 1 || value > 65535) {
                    fprintf(stderr, "Error: Invalid HTTP port number\n");
                    return EXIT_FAILURE;
                }
                arg_TCP_Port = (uint16_t)value;  // In range, checked above
                break;
            case 'S':
                opt_server = optarg;
                break;
            case 'P':
                arg_MQTT_Port = atoi(optarg);
                if (arg_MQTT_Port < 1 || arg_MQTT_Port > 65535) {
                    fprintf(stderr, "Error: Invalid MQTT port number\n");
                    return EXIT_FAILURE;
                }
                break;
            case 'U':
                opt_login = optarg;
                break;
            case 'W':
                opt_password = optarg;
                break;
            case 's':
                arg_AV_Samples = atoi(optarg);
                break;
            case 'a':
                value = atoi(optarg);
                if (value < 0 || value > 255) {
                    fprintf(stderr, "Error: Invalid inverter bus address (0 to 255)\n");
                    return EXIT_FAILURE;
                }
                arg_InverterAddr = (uint8_t)value;  // In range, checked above
                break;
            case 'l':
                arg_LogFile = optarg;
                break;
            case 'L':
                arg_LogLevel = atoi(optarg);
                break;
            case 'x':
                arg_TestMode = 1;
                break;
            case ':':
                fprintf (stderr, "Option -%c requires an argument.\n", optopt);
                return EXIT_FAILURE;
            case '?':
                if (isprint (optopt))
                    fprintf (stderr, "Unknown option '-%c'.\n", optopt);
                else
                    fprintf (stderr, "Unknown option character '0x%x'.\n", (unsigned char)optopt);
                return EXIT_FAILURE;
            default:
                abort();
        }
    }
    
    // Login and password from the environment (e.g. EnvironmentFile of the systemd unit), unless -U or -W is given
    if (opt_login == NULL) opt_login = getenv("SOLAXD_MQTT_LOGIN");
    if (opt_password == NULL) opt_password = getenv("SOLAXD_MQTT_PASSWORD");

    if (arg_LogFile == NULL)
    {
        fp_log_file = stderr;
    }
    else
    {
        fp_log_file = fopen(arg_LogFile, "a");    // open Log-File
        if (fp_log_file == NULL) { printf("Main: Error opening Log-File '%s': %s\n", arg_LogFile, strerror(errno)); return EXIT_FAILURE; }
    }
    if (arg_LogLevel >= LOG_NOTICE) fd_stop_log = fileno(fp_log_file);

    NOTICE_MESSAGE("Main: %s started", SOLARXD_STRING);
    
    INFO_MESSAGE("Main: TTY_Device   : %s", arg_TTY_Device  );
    INFO_MESSAGE("Main: TCP_Port     : %d", arg_TCP_Port    );
    INFO_MESSAGE("Main: AV_Samples   : %d", arg_AV_Samples  );
    INFO_MESSAGE("Main: InverterAddr : %d", arg_InverterAddr);
    INFO_MESSAGE("Main: LogFile      : %s", arg_LogFile     );
    INFO_MESSAGE("Main: LogLevel     : %d", arg_LogLevel    );
    INFO_MESSAGE("Main: TestMode     : %d", arg_TestMode    );
    
    // Copied here, after the last early return, so that every copy is freed by cleanup()
    arg_MQTT_Server   = strdup(opt_server   != NULL ? opt_server   : DEFAULT_MQTT_SERVER);
    arg_MQTT_Login    = strdup(opt_login    != NULL ? opt_login    : DEFAULT_MQTT_LOGIN);
    arg_MQTT_Password = strdup(opt_password != NULL ? opt_password : DEFAULT_MQTT_PASSWORD);

    int exit_code = EXIT_FAILURE;
    if (arg_MQTT_Server == NULL || arg_MQTT_Login == NULL || arg_MQTT_Password == NULL) {
        ERROR_MESSAGE("Main: Out of memory while copying the MQTT settings");
    } else {
        exit_code = run_daemon();  // Every return of run_daemon() goes through cleanup()
    }
    cleanup();
    return exit_code;
}