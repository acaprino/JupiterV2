//+------------------------------------------------------------------+
//|                              EconomicCalendar_Service.mq5         |
//|                          Copyright 2024, Alfio Caprino.           |
//|                                https://www.mql5.com               |
//+------------------------------------------------------------------+
#property service
#property copyright "Copyright 2024, Alfio Caprino."
#property link      "https://www.mql5.com"
#property version   "1.00"

#define CAL_FILE_NAME "economic_calendar.json"
#define LOCK_FILE_NAME "lock.sem"

//+------------------------------------------------------------------+
//| Service Entry Point                                              |
//+------------------------------------------------------------------+
void OnStart()
  {
// Infinite loop for continuous execution
   do
     {
      // Create the lock file to signal that the calendar is being generated
      CreateLockFile();

      // Write the economic calendar (or perform another task)
      WriteCalendar();

      // Delete the lock file after calendar generation is complete
      DeleteLockFile();

      // Calculate how many seconds until the next full hour
      datetime now = TimeCurrent();
      int seconds_past_hour = now % 3600; // Seconds since the last full hour
      int seconds_until_next_hour = 3600 - seconds_past_hour; // Seconds until the next full hour

      // Wait until the next full hour
      Sleep(seconds_until_next_hour * 1000);
     }
   while(true);
  }

//+------------------------------------------------------------------+
//| Create the lock file (lock.sem)                                  |
//+------------------------------------------------------------------+
void CreateLockFile()
  {
   int lockFile = FileOpen(LOCK_FILE_NAME, FILE_WRITE | FILE_TXT);
   if(lockFile != INVALID_HANDLE)
     {
      FileWrite(lockFile, "Calendar generation in progress");
      FileClose(lockFile);
      Print("[INFO] Lock file created: ", LOCK_FILE_NAME);
     }
   else
     {
      Print("[ERROR] Unable to create lock file: ", LOCK_FILE_NAME);
     }
  }

//+------------------------------------------------------------------+
//| Delete the lock file (lock.sem)                                  |
//+------------------------------------------------------------------+
void DeleteLockFile()
  {
   if(FileDelete(LOCK_FILE_NAME))
      Print("[INFO] Lock file deleted: ", LOCK_FILE_NAME);
   else
      Print("[ERROR] Unable to delete lock file: ", LOCK_FILE_NAME);
  }

//+------------------------------------------------------------------+
//| Event structure definition                                       |
//+------------------------------------------------------------------+
struct Event
  {
   int               country_id;
   string            country_name;
   string            country_code;
   string            country_currency;
   string            country_currency_symbol;
   string            country_url_name;
   int               event_id;
   int               event_type;
   int               event_sector;
   int               event_frequency;
   int               event_time_mode;
   int               event_unit;
   int               event_importance;
   int               event_multiplier;
   int               event_digits;
   string            event_source_url;
   string            event_code;
   string            event_name;
   datetime          event_time;
   int               event_period;
   int               event_revision;
   double            actual_value;
   double            prev_value;
   double            revised_prev_value;
   double            forecast_value;
   int               impact_type;
  };

//+------------------------------------------------------------------+
//| Write the economic calendar to a JSON file                       |
//+------------------------------------------------------------------+
void WriteCalendar()
  {
// Delete existing calendar file to start fresh
   if(FileIsExist(CAL_FILE_NAME))
      FileDelete(CAL_FILE_NAME);

// Open the file for reading and writing, report error if fails
   int textFile = FileOpen(CAL_FILE_NAME, FILE_READ | FILE_WRITE | FILE_ANSI | FILE_TXT);
   if(textFile == INVALID_HANDLE)
     {
      Alert("Error opening file");
      return;
     }

// Calculate start and end dates (from yesterday to 2 days ahead)
   datetime today = RemoveTimeFromDatetime(TimeCurrent());
   datetime startDateTime = today;
   datetime endDateTime = AddMonths(today, 1);

   Print("Writing events from ", FormatDateTime(startDateTime), " to ", FormatDateTime(endDateTime));

// Get the list of countries for filtering events
   MqlCalendarCountry countries[];
   int countries_count = CalendarCountries(countries);

// Array to store events
   Event myEvents[];

// Fetch events for each country within the specified date range
   for(int i = 0; i < countries_count; i++)
     {
      MqlCalendarEvent events[];
      int event_count = CalendarEventByCountry(countries[i].code, events);

      if(event_count <= 0)
         continue;

      for(int j = 0; j < event_count; j++)
        {
         MqlCalendarValue values[];
         int value_count = CalendarValueHistoryByEvent(events[j].id, values, startDateTime, endDateTime);

         if(value_count <= 0)
            continue;

         // Populate Event structures and store them in the array
         for(int k = 0; k < value_count; k++)
           {
            Event myEvent;
            PopulateEvent(myEvent, countries[i], events[j], values[k]);
            ArrayResize(myEvents, ArraySize(myEvents) + 1);
            myEvents[ArraySize(myEvents) - 1] = myEvent;
           }
        }
     }

// Serialize events to JSON and write to the file
   string eventsJson = SerializeEventsToJson(myEvents);
   WriteTextFile(textFile, eventsJson);

// Close the file
   FileClose(textFile);
  }

//+------------------------------------------------------------------+
//| Write text to the file                                           |
//+------------------------------------------------------------------+
void WriteTextFile(int textFile, string text)
  {
   FileSeek(textFile, 0, SEEK_END); // Move to end of the file
   FileWrite(textFile, text);       // Write the text string
  }

//+------------------------------------------------------------------+
//| Populate an Event structure with data                            |
//+------------------------------------------------------------------+
void PopulateEvent(Event &myEvent, MqlCalendarCountry &country, MqlCalendarEvent &event, MqlCalendarValue &value)
  {
   myEvent.country_id            = country.id;
   myEvent.country_name          = country.name;
   myEvent.country_code          = country.code;
   myEvent.country_currency      = country.currency;
   myEvent.country_currency_symbol = country.currency_symbol;
   myEvent.country_url_name      = country.url_name;
   myEvent.event_id              = event.id;
   myEvent.event_type            = event.type;
   myEvent.event_sector          = event.sector;
   myEvent.event_frequency       = event.frequency;
   myEvent.event_time_mode       = event.time_mode;
   myEvent.event_unit            = event.unit;
   myEvent.event_importance      = event.importance;
   myEvent.event_multiplier      = event.multiplier;
   myEvent.event_digits          = event.digits;
   myEvent.event_source_url      = event.source_url;
   myEvent.event_code            = event.event_code;
   myEvent.event_name            = event.name;
   myEvent.event_time            = value.time;
   myEvent.event_period          = value.period;
   myEvent.event_revision        = value.revision;
   myEvent.actual_value          = value.actual_value;
   myEvent.prev_value            = value.prev_value;
   myEvent.revised_prev_value    = value.revised_prev_value;
   myEvent.forecast_value        = value.forecast_value;
   myEvent.impact_type           = value.impact_type;
  }

//+------------------------------------------------------------------+
//| Serialize an Event structure to JSON format                      |
//+------------------------------------------------------------------+
string SerializeEventToJson(const Event &e)
  {
   return StringFormat(
             "{\"country_id\":%d,\"country_name\":\"%s\",\"country_code\":\"%s\","
             "\"country_currency\":\"%s\",\"country_currency_symbol\":\"%s\","
             "\"country_url_name\":\"%s\",\"event_id\":%d,\"event_type\":%d,"
             "\"event_sector\":%d,\"event_frequency\":%d,\"event_time_mode\":%d,"
             "\"event_unit\":%d,\"event_importance\":%d,\"event_multiplier\":%d,"
             "\"event_digits\":%d,\"event_source_url\":\"%s\",\"event_code\":\"%s\","
             "\"event_name\":\"%s\",\"event_time\":\"%s\",\"event_period\":%d,"
             "\"event_revision\":%d,\"actual_value\":%.2f,\"prev_value\":%.2f,"
             "\"revised_prev_value\":%.2f,\"forecast_value\":%.2f,\"impact_type\":%d}",
             e.country_id, e.country_name, e.country_code, e.country_currency,
             e.country_currency_symbol, e.country_url_name, e.event_id,
             e.event_type, e.event_sector, e.event_frequency, e.event_time_mode,
             e.event_unit, e.event_importance, e.event_multiplier, e.event_digits,
             e.event_source_url, e.event_code, EscapeDoubleQuotes(e.event_name),
             TimeToString(e.event_time, TIME_DATE|TIME_MINUTES), e.event_period,
             e.event_revision, e.actual_value, e.prev_value, e.revised_prev_value,
             e.forecast_value, e.impact_type);
  }

//+------------------------------------------------------------------+
//| Serialize an array of Event structures into a JSON array         |
//+------------------------------------------------------------------+
string SerializeEventsToJson(const Event &myEvents[])
  {
   string jsonArray = "["; // Start JSON array
   for(int i = 0; i < ArraySize(myEvents); i++)
     {
      if(i > 0)
         jsonArray += ","; // Add comma between objects
      jsonArray += SerializeEventToJson(myEvents[i]); // Serialize each event
     }
   jsonArray += "]"; // End JSON array
   return jsonArray; // Return serialized JSON array
  }

//+------------------------------------------------------------------+
//| Utility to format a datetime value into DD/MM/YYYY format        |
//+------------------------------------------------------------------+
string FormatDateTime(datetime time)
  {
   MqlDateTime dt;
   TimeToStruct(time, dt);
   return StringFormat("%02d/%02d/%04d", dt.day, dt.mon, dt.year);
  }

//+------------------------------------------------------------------+
//| Utility to remove the time part of a datetime                    |
//+------------------------------------------------------------------+
datetime RemoveTimeFromDatetime(datetime time)
  {
   MqlDateTime dt;
   TimeToStruct(time, dt);
   dt.hour = 0;
   dt.min = 0;
   dt.sec = 0;
   return StructToTime(dt);
  }

//+------------------------------------------------------------------+
//| Utility to escape double quotes for JSON compatibility           |
//+------------------------------------------------------------------+
string EscapeDoubleQuotes(string text)
  {
   StringReplace(text, "\"", "\\\"");
   return text;
  }

//+------------------------------------------------------------------+
//| Add or subtract days from a given datetime                       |
//+------------------------------------------------------------------+
datetime AddDays(datetime currentDate, int daysToAdjust)
  {
// Convert datetime into MqlDateTime structure
   MqlDateTime structDate;
   TimeToStruct(currentDate, structDate);

// Days in each month
   int daysInMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

// Leap year check for February
   if(structDate.mon == 2 && ((structDate.year % 4 == 0 && structDate.year % 100 != 0) || (structDate.year % 400 == 0)))
      daysInMonth[1] = 29;

// Add or subtract days
   structDate.day += daysToAdjust;

// Handle overflow and underflow of days
   while(structDate.day > daysInMonth[structDate.mon - 1])   // Overflow case
     {
      structDate.day -= daysInMonth[structDate.mon - 1];
      structDate.mon++;
      if(structDate.mon > 12)
        {
         structDate.mon = 1;
         structDate.year++;
        }

      // Adjust for leap year if month is February
      if(structDate.mon == 2 && ((structDate.year % 4 == 0 && structDate.year % 100 != 0) || (structDate.year % 400 == 0)))
         daysInMonth[1] = 29;
      else
         daysInMonth[1] = 28;
     }

   while(structDate.day <= 0)   // Underflow case
     {
      structDate.mon--;
      if(structDate.mon < 1)
        {
         structDate.mon = 12;
         structDate.year--;
        }

      // Adjust for leap year if month is February
      if(structDate.mon == 2 && ((structDate.year % 4 == 0 && structDate.year % 100 != 0) || (structDate.year % 400 == 0)))
         daysInMonth[1] = 29;
      else
         daysInMonth[1] = 28;

      structDate.day += daysInMonth[structDate.mon - 1];
     }

// Convert the modified structure back to datetime
   datetime adjustedDate = StructToTime(structDate);
   return adjustedDate;  // Return the adjusted datetime
  }
//+------------------------------------------------------------------+
//| Add or subtract months from a given datetime                     |
//+------------------------------------------------------------------+
datetime AddMonths(datetime currentDate, int monthsToAdjust)
  {
   MqlDateTime structDate;
   TimeToStruct(currentDate, structDate); // Converts the datetime into a MqlDateTime structure

   structDate.mon += monthsToAdjust; // Adjusts the month by the specified amount

   while(structDate.mon > 12) // Handle case for month overflow
     {
      structDate.mon -= 12; // Decreases the month by 12
      structDate.year += 1; // Moves to the next year
     }

   while(structDate.mon <= 0) // Handle case for month underflow
     {
      structDate.mon += 12; // Increases the month by 12
      structDate.year -= 1; // Moves to the previous year
     }

// Handles cases where the day is not valid for the new month
   int daysInMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
// Leap year check for February
   if(structDate.mon == 2 && ((structDate.year % 4 == 0 && structDate.year % 100 != 0) || (structDate.year % 400 == 0)))
      daysInMonth[1] = 29; // February has 29 days in a leap year

   if(structDate.day > daysInMonth[structDate.mon - 1])
     {
      structDate.day = daysInMonth[structDate.mon - 1]; // Adjusts the day to the last day of the month if necessary
     }

   datetime adjustedDate = StructToTime(structDate); // Converts the modified structure back into datetime

   return adjustedDate; // Returns the adjusted datetime
  }

//+------------------------------------------------------------------+
