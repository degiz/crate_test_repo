import csv


class Inserter:
    """
    Base class for each new table information insertion
    """

    table_name = ""
    drop_table_sql = ""
    create_table_sql = ""
    prepared_insert_statement_sql = ""

    def __init__(self):
        self.drop_table_sql = "DROP TABLE IF EXISTS {0}".format(self.table_name)

    def process_custom_input_data(self, input_data):
        """
        This method is used to fix inconsistent input data
        """
        return input_data

    def execute(self, filename, cursor):
        """
        This method reads input data and performs Drop/Create/Insert operations
        """

        # remove previous information
        cursor.execute(self.drop_table_sql)
        cursor.execute(self.create_table_sql)

        with open(filename) as csv_file:
            reader = csv.reader(csv_file)
            # skip the header
            next(reader)

            # setup batch execution
            batch = []

            for row in reader:

                row = self.process_custom_input_data(row)

                if len(batch) < 1000:
                    batch.append(row)
                else:
                    cursor.executemany(self.prepared_insert_statement_sql, batch)
                    batch = []

            if len(batch) != 0:
                cursor.executemany(self.prepared_insert_statement_sql, batch)


class AgencyInserter(Inserter):
    table_name = "agency"

    def process_custom_input_data(self, input_data):
        input_data[0] = int(input_data[0])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "agency_id integer PRIMARY KEY," \
                       "agency_name text," \
                       "agency_url text," \
                       "agency_timezone text," \
                       "agency_lang text," \
                       "agency_phone text" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "agency_id," \
                                    "agency_name," \
                                    "agency_url," \
                                    "agency_timezone," \
                                    "agency_lang," \
                                    "agency_phone" \
                                    ") VALUES (?, ?, ?, ?, ?, ?)".format(table_name)


class CalendarInserter(Inserter):
    table_name = "calendar"

    def process_custom_input_data(self, input_data):
        for i in range(0, len(input_data)):
            input_data[i] = int(input_data[i])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "service_id integer PRIMARY KEY," \
                       "monday boolean," \
                       "tuesday boolean," \
                       "wednesday boolean," \
                       "thursday boolean," \
                       "friday boolean," \
                       "saturday boolean," \
                       "sunday boolean," \
                       "start_date integer," \
                       "end_date integer" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "service_id," \
                                    "monday," \
                                    "tuesday," \
                                    "wednesday," \
                                    "thursday," \
                                    "friday," \
                                    "saturday," \
                                    "sunday," \
                                    "start_date," \
                                    "end_date" \
                                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(table_name)


class CalendarDatesInserter(Inserter):
    table_name = "calendar_dates"

    def process_custom_input_data(self, input_data):
        for i in range(0, len(input_data)):
            input_data[i] = int(input_data[i])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "service_id integer PRIMARY KEY," \
                       "date integer PRIMARY KEY," \
                       "exception_type integer" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "service_id," \
                                    "date," \
                                    "exception_type" \
                                    ") VALUES (?, ?, ?)".format(table_name)


class FrequenciesInserter(Inserter):
    table_name = "frequencies"

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "trip_id integer PRIMARY KEY," \
                       "start_time integer," \
                       "end_time integer," \
                       "headway_secs integer," \
                       "exact_times integer" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "trip_id," \
                                    "start_time," \
                                    "end_time" \
                                    "headway_secs," \
                                    "exact_times" \
                                    ") VALUES (?, ?, ?, ?, ?)".format(table_name)


class RoutesInserter(Inserter):
    table_name = "routes"

    def process_custom_input_data(self, input_data):
        input_data[1] = int(input_data[1])
        input_data[4] = int(input_data[4])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "route_id text PRIMARY KEY," \
                       "agency_id integer," \
                       "route_short_name text," \
                       "route_long_name text," \
                       "route_type integer," \
                       "route_color text," \
                       "route_text_color text," \
                       "route_desc text" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "route_id," \
                                    "agency_id," \
                                    "route_short_name," \
                                    "route_long_name," \
                                    "route_type," \
                                    "route_color," \
                                    "route_text_color," \
                                    "route_desc" \
                                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)".format(table_name)


class ShapesInserter(Inserter):
    table_name = "shapes"

    def process_custom_input_data(self, input_data):
        input_data[0] = int(input_data[0])
        input_data[1] = float(input_data[1])
        input_data[2] = float(input_data[2])
        input_data[3] = int(input_data[3])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "shape_id integer," \
                       "point geo_point," \
                       "shape_pt_sequence integer" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "shape_id," \
                                    "point," \
                                    "shape_pt_sequence" \
                                    ") VALUES (?, [?, ?], ?)".format(table_name)


class StopTimesInserter(Inserter):
    table_name = "stop_times"

    def process_custom_input_data(self, input_data):
        input_data[0] = int(input_data[0])
        input_data[4] = int(input_data[4])
        input_data[5] = int(input_data[5])
        input_data[6] = int(input_data[6])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "trip_id integer," \
                       "arrival_time text," \
                       "departure_time text," \
                       "stop_id text," \
                       "stop_sequence integer," \
                       "pickup_type integer," \
                       "drop_off_type integer," \
                       "stop_headsign text" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "trip_id," \
                                    "arrival_time," \
                                    "departure_time," \
                                    "stop_id," \
                                    "stop_sequence," \
                                    "pickup_type," \
                                    "drop_off_type," \
                                    "stop_headsign" \
                                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)".format(table_name)


class StopsInserter(Inserter):
    table_name = "stops"

    def process_custom_input_data(self, input_data):
        input_data[0] = int(input_data[0])
        input_data[4] = float(input_data[4])
        input_data[5] = float(input_data[5])
        input_data[6] = int(input_data[6])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "stop_id bigint PRIMARY KEY," \
                       "stop_code text," \
                       "stop_name text," \
                       "stop_desc text," \
                       "point geo_point," \
                       "location_type integer," \
                       "parent_station text," \
                       "wheelchair_boarding text," \
                       "platform_code text," \
                       "zone_id text" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "stop_id," \
                                    "stop_code," \
                                    "stop_name," \
                                    "stop_desc," \
                                    "point," \
                                    "location_type," \
                                    "parent_station," \
                                    "wheelchair_boarding," \
                                    "platform_code," \
                                    "zone_id" \
                                    ") VALUES (?, ?, ?, ?, [?, ?], ?, ?, ?, ?, ?)".format(table_name)


class TransfersInserter(Inserter):
    table_name = "transfers"

    def process_custom_input_data(self, input_data):
        input_data[0] = int(input_data[0])
        input_data[1] = float(input_data[1])
        input_data[2] = float(input_data[2])
        if input_data[3] == "":
            input_data[3] = 0
        else:
            input_data[3] = int(input_data[3])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "from_stop_id bigint," \
                       "to_stop_id bigint," \
                       "transfer_type integer," \
                       "min_transfer_time integer," \
                       "from_route_id text," \
                       "to_route_id text," \
                       "from_trip_id text," \
                       "to_trip_id text" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "from_stop_id," \
                                    "to_stop_id," \
                                    "transfer_type," \
                                    "min_transfer_time," \
                                    "from_route_id," \
                                    "to_route_id," \
                                    "from_trip_id," \
                                    "to_trip_id" \
                                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)".format(table_name)


class TripsInserter(Inserter):
    table_name = "trips"

    def process_custom_input_data(self, input_data):
        input_data[1] = int(input_data[1])
        input_data[2] = int(input_data[2])
        input_data[5] = int(input_data[5])
        input_data[7] = int(input_data[7])
        if input_data[8] == "":
            input_data[8] = 0
        else:
            input_data[8] = int(input_data[8])
        if input_data[9] == "":
            input_data[9] = 0
        else:
            input_data[9] = int(input_data[9])
        return input_data

    create_table_sql = "CREATE TABLE IF NOT EXISTS {0} (" \
                       "route_id text," \
                       "service_id integer," \
                       "trip_id integer," \
                       "trip_headsign text," \
                       "trip_short_name text," \
                       "direction_id integer," \
                       "block_id text," \
                       "shape_id integer," \
                       "wheelchair_accessible integer," \
                       "bikes_allowed boolean" \
                       ")".format(table_name)

    prepared_insert_statement_sql = "INSERT INTO {0} (" \
                                    "route_id," \
                                    "service_id," \
                                    "trip_id," \
                                    "trip_headsign," \
                                    "trip_short_name," \
                                    "direction_id," \
                                    "block_id," \
                                    "shape_id," \
                                    "wheelchair_accessible," \
                                    "bikes_allowed" \
                                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)".format(table_name)


INSERTERS = {
    klass.table_name: klass
    for name, klass in globals().items()
    if name.endswith('Inserter') and name != 'Inserter'
}
