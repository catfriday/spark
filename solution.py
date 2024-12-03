import csv


# create reusable csv reader function
def read_data(filename):
    with open(filename, "r") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


# create a function to filter the zip codes from zips.csv that are in slcsp.csv
def get_filtered_zip_codes():
    filtered_data = []
    zips_from_slcsp = [int(row["zipcode"]) for row in read_data("slcsp.csv")]
    zips = [row for row in read_data("zips.csv")]

    for row in zips:
        if int(row["zipcode"]) in zips_from_slcsp:
            filtered_data.append(row)

    return filtered_data


# get only the silver plans from plans.csv that are in the rate areas from the
# filtered zip codes
def get_filtered_silver_plans():
    filtered_data = []
    zips = get_filtered_zip_codes()
    rate_areas_from_zips = list(set([row["rate_area"] for row in zips]))

    for row in read_data("plans.csv"):
        if (
            row["metal_level"].lower() == "silver"
            and row["rate_area"] in rate_areas_from_zips
        ):
            filtered_data.append(row)

    return filtered_data


# get mapping of rate area to slcsp rate
# ex. {'1': '226.84', '2': '166.13'}
def get_rate_area_to_slcsp_rate():
    rate_area_to_rate = {}
    silver_plans = get_filtered_silver_plans()

    for row in silver_plans:
        if row["rate_area"] not in rate_area_to_rate:
            rate_area_to_rate[row["rate_area"]] = []
        rate_area_to_rate[row["rate_area"]].append(row["rate"])

    for key, value in rate_area_to_rate.items():
        # sort the list and set rates to first 2 elements of the list to get
        # the 2nd lowest rate
        rate_area_to_rate[key] = sorted(value)[:2]

        # if there is only 0-1 rate or if there are 2 equal rates, we can't
        # determine the 2nd lowest rate
        if (
            len(rate_area_to_rate[key]) > 1
            and rate_area_to_rate[key][0] == rate_area_to_rate[key][1]
            or len(rate_area_to_rate[key]) <= 1
        ):
            rate_area_to_rate[key] = ""
        else:
            rate_area_to_rate[key] = rate_area_to_rate[key][1]

    return rate_area_to_rate


# get mapping of zip code to rate
# ex. {'64148': '226.84', '64149': '166.13'}
def get_zip_to_slcsp_rate():
    zip_to_rate_area = {}
    zips = get_filtered_zip_codes()

    for row in zips:
        if row["zipcode"] not in zip_to_rate_area:
            zip_to_rate_area[row["zipcode"]] = []
        zip_to_rate_area[row["zipcode"]].append(row["rate_area"])

    # only zip codes in 1 rate area can be used to determine the rate
    filtered_zip_to_rate_area = {}
    rate_area_to_rate = get_rate_area_to_slcsp_rate()
    for zip, rate_area in zip_to_rate_area.items():
        if len(list(set(rate_area))) == 1:
            filtered_zip_to_rate_area[zip] = rate_area_to_rate[rate_area[0]]

    return filtered_zip_to_rate_area


# append the slcsp rate to the slcsp.csv rate column
def add_rate_to_csv(filename):
    zip_to_rate = get_zip_to_slcsp_rate()
    slcsp_data = read_data("slcsp.csv")
    header = ["zipcode", "rate"]

    for row in slcsp_data:
        if row["zipcode"] in zip_to_rate:
            row["rate"] = zip_to_rate[row["zipcode"]]

    with open(filename, "w") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in slcsp_data:
            writer.writerow(row)


# emit data from csv file to stdout
def emit_data_to_stdout(file_to_read):
    with open(file_to_read, "r") as f:
        reader = csv.reader(f)

        with open("standard_output.txt", "w") as f:
            for row in reader:
                print(row[0] + "," + row[1], file=f)
                print(row[0] + "," + row[1])


FILE_NAME = "slcsp.csv"


def main():
    add_rate_to_csv(FILE_NAME)
    emit_data_to_stdout(FILE_NAME)


if __name__ == "__main__":
    main()
