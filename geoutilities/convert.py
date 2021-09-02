
from collections import defaultdict
from os import scandir
from pathlib import Path
from zipfile import is_zipfile, ZipFile
from gdal import ogr
from os import path, popen, mkdir
import getpass
from pycrs.parse import from_epsg_code
import tempfile
from pgpy import PGPMessage


# reads  directory given for (zipped)shp files, geojson, and gpkg files. WON'T OPEN FOLDERS CONTAINED IN GIVEN DIRECTORY
def gathergeo(fileordirectory: str) -> dict:
    filesdict = defaultdict(list)

    fileordirectory = Path(fileordirectory)
    try:
        if path.isfile(fileordirectory):
            file = fileordirectory
            if is_zipfile(file):
                try:
                    with ZipFile(file) as unzipped:
                        for unzipped_file in unzipped.namelist():

                            if unzipped_file.endswith(".shp"):
                                filesdict["shp"].append(file)
                                break
                except:
                    print("Invalid Zip")

            elif file.name.endswith(".geojson"):
                filesdict["geojson"].append(Path(file))
            elif file.name.endswith(".gpkg"):
                filesdict["gpkg"].append(Path(file))

        elif path.isdir(fileordirectory):
            directory = fileordirectory
            try:
                for file in scandir(path=directory):
                    if is_zipfile(file.path):
                        try:
                            with ZipFile(file.path) as unzipped:
                                for unzipped_file in unzipped.namelist():

                                    if unzipped_file.endswith(".shp"):
                                        filesdict["shp"].append(Path(file.path))
                                        break
                        except Exception as e:
                            print("Invalid Zip? " + str(e))

                    elif file.path.endswith(".geojson"):
                        filesdict["geojson"].append(Path(file.path))
                    elif file.path.endswith(".gpkg"):
                        filesdict["gpkg"].append(Path(file.path))


            except:
                print(str(directory) + " is not valid")
    except:
        print(str(fileordirectory) + " is not valid")

    return filesdict

# uses convert.gathergeo() to generate the file dict and returns the dict instead as a list if Paths (e.g. Windows or POSIX)
def filesList(fileordirectory: str) -> list:

    fileL= list()
    files = gathergeo(fileordirectory).values()

    for key in files:
        for val in key:
            print(val)
            fileL.append(val)

    return fileL

# Converts a file or folder of files into the chosen geo-file
def convertToGenericGeoGEN(geoformat: str, fileordirectory: str):
    files = gathergeo(fileordirectory.strip())
    geoformat = geoformat.upper().strip()
    driver = ""
    zipped = ""
    srs_decided = False
    driver_chosen = False
    output_srs = ""

    print(geoformat + " is the format")

    if geoformat != "POSTGIS":
        while not srs_decided:
            output_srs = input("Transform to EPSG 4269 or 4326 or EXIT: ")
            if "4269" in output_srs:
                output_srs = "EPSG:4269"
                srs_decided = True
            elif "4326" in output_srs:
                output_srs = "EPSG:4326"
                srs_decided = True
            elif output_srs.upper() == "EXIT":
                break

    elif geoformat == "POSTGIS":
        output_srs = "EPSG:4236"
        srs_decided = True

    while not driver_chosen:
        if geoformat == "SHP":
            zipped = ".zip"
            driver = "ESRI Shapefile"
            driver_chosen = True
        elif geoformat == "GEOJSON":
            driver = "GeoJSON"
            driver_chosen = True
        elif geoformat == "GPKG":
            driver = geoformat
            driver_chosen = True
        elif geoformat == "POSTGIS":
            driver = "PostgreSQL"
            driver_chosen = True
        elif geoformat.upper() == "EXIT":
            break

        else:
            geoformat = input("You can only export to shp, geojson, gpkg, or PostGIS. Please pick one or EXIT: ").upper()


    if srs_decided and driver_chosen:

        # Shapefiles will all be zipped together.  The driver used to convert is assigned.
        # Converted files go to: thisprogramsfolder/data/converted

        if geoformat in ["SHP", "GEOJSON", "GPKG"]:
            try:

                output_destination = str(Path().absolute().parent) + "/data/converted"
                outputFolder(output_destination)

                source_srs: str
                filetype: object
                filepath: Path
                if output_destination:
                    for filetype in files:

                        if str(filetype).upper() != geoformat:

                            for filepath in files[filetype]:
                                filepath = Path(filepath)
                                conversionFunction(filepath, output_destination, driver, geoformat, output_srs, zipped)

                        else:
                            reproject = input("Would you like to reproject " + geoformat + " file[s]? YES/NO?: ")

                            if reproject.upper() == "YES":
                                for filepath in files[filetype]:
                                    filepath = Path(filepath)
                                    source_srs = checkSRS(filepath)

                                    if source_srs != output_srs and source_srs != "":
                                        conversionFunction(filepath, output_destination, driver, geoformat, output_srs, zipped,
                                                           source_srs=source_srs)
                                    else:
                                        print(f"{str(filepath.name)} is in {output_srs} and won't be converted")

            except:
                print("to shapefile ERROR")

        elif geoformat == "POSTGIS":
            print("IN POSTGIS UPLOAD")
            user = input("USERNAME: ")
            password = getpass.getpass(prompt="PASSPHRASE PLEASE:")
            for filetype in files:
                print("FILETYPE " + str(filetype))
                print(geoformat)

                if str(filetype).upper() != geoformat:

                    for filepath in files[filetype]:
                        filepath = Path(filepath)
                        print("FILEPATH " + str(filepath) + " will be uploaded")

                        uploadFunction(filepath, user, password)

    else:
        print("No conversion will take place")


# Called by convertToGenericGeoGEN
# The conversion function using ogr2ogr to convert formats
def conversionFunction(filepath: Path, output_destination: str, driver: str, geoformat: str, output_srs: str,
                       zipped: str, **source_srs: str) -> None:
    try:
        if source_srs == {} or None:
            source_srs = checkSRS(filepath)
        if source_srs:
            filename = filepath.name.rsplit(".")[0]

            if filepath.name.endswith(".zip"):
                unzip = "/vsizip//"
            else:
                unzip = ""

            command = f'ogr2ogr -f "{driver}" -s_srs {source_srs} -t_srs {output_srs} ' \
                      f'"{Path(output_destination + "/" + filename + "." + geoformat.lower() + zipped)}" {unzip + str(filepath)} '

            # Run the command
            popen(command).read()
        else:
            print(f"{filepath.name} cannot be converted without a projection")

    except Exception as e:
        print("CONVERSION ERROR:")
        print(str(e))


# Checks and extracts the CRS for use in various functions
def checkSRS(filepath: Path) -> str:
    # Spatial reference system. Sometimes referred to as Coordinate Reference System

    spatial_rs = ""

    # unzip if zipfile
    if is_zipfile(filepath):
        vsizip = "/vsizip/"
    else:
        vsizip = ""

    try:
        file = ogr.Open(f"{vsizip}{filepath}")
        layer = file.GetLayer()
        spatial_ref = layer.GetSpatialRef()

        if spatial_ref != None:
            spatial_rs = spatial_ref.GetAuthorityName(None) + ":" + spatial_ref.GetAuthorityCode(None)
            # print(spatial_rs + " WAS looked up")

        # Catch a wonky "None" SRS ref that may have been assigned by whatever program used by the county.
        if "None" in spatial_rs:
            print("spatial to blank")
            spatial_rs = ""
        # print("AFTER IF SPATIAL RS= ****" + spatial_rs + "****")

    except Exception as e:
        print("DOESNT HAVE: " + str(e))

    try:
        if not spatial_rs:
            print("ABOUT to search")
            filename = filepath.name.rsplit(".")[0]
            spatial_rs = searchForCode(filename)

            if not spatial_rs:
                spatial_rs = input("What is the source coordinate reference in EPSG (e.g. 3734) ? Else leave blank: ")

                if not from_epsg_code(spatial_rs):
                    print("That code was invalid")
                    spatial_rs = ""
                else:
                    # print("ERROR 1")
                    spatial_rs = "EPSG:" + spatial_rs

    except Exception as e:
        print(str(e))

    return spatial_rs


# Create an outputfolder in this programs main folder called "converted" for the output of converted files
def outputFolder(pathgiven: str) -> bool:
    if Path(pathgiven).exists():
        return True
    else:
        try:
            mkdir(Path(pathgiven))
            return True
        except Exception as e:
            print(e)
            return False



# Searches for the county name to get its state plane equivalent in EPSG
def searchForCode(toconvert: str) -> str:

    # The state plane codes that the county governments will use if the parcel data wasn't obtained from an ArcGis
    # REST service.
    newcodes = {'adams': 3735, 'allen': 3734, 'ashland': 3734, 'ashtabula': 3734, 'athens': 3735, 'auglaize': 3734,
                'belmont': 3735, 'brown': 3735, 'butler': 3735, 'carroll': 3734, 'champaign': 3735, 'clark': 3735,
                'clermont': 3735, 'clinton': 3735, 'columbiana': 3734, 'coshocton': 3734, 'crawford': 3734,
                'cuyahoga': 3734, 'darke': 3735, 'defiance': 3734, 'delaware': 3734, 'erie': 3734, 'fairfield': 3735,
                'fayette': 3735, 'franklin': 3735, 'fulton': 3734, 'gallia': 3735, 'geauga': 3734, 'greene': 3735,
                'guernsey': 3735, 'hamilton': 3735, 'hancock': 3734, 'hardin': 3734, 'harrison': 3734, 'henry': 3734,
                'highland': 3735, 'hocking': 3735, 'holmes': 3734, 'huron': 3734, 'jackson': 3735, 'jefferson': 3734,
                'knox': 3734, 'lake': 3734, 'lawrence': 3735, 'licking': 3735, 'logan': 3734, 'lorain': 3734,
                'lucas': 3734, 'madison': 3735, 'mahoning': 3734, 'marion': 3734, 'medina': 3734, 'meigs': 3735,
                'mercer': 3734, 'miami': 3735, 'monroe': 3735, 'montgomery': 3735, 'morgan': 3735, 'morrow': 3734,
                'muskingum': 3735, 'noble': 3735, 'ottawa': 3734, 'paulding': 3734, 'perry': 3735, 'pickaway': 3735,
                'pike': 3735, 'portage': 3734, 'preble': 3735, 'putnam': 3734, 'richland': 3734, 'ross': 3735,
                'sandusky': 3734, 'scioto': 3735, 'seneca': 3734, 'shelby': 3734, 'stark': 3734, 'summit': 3734,
                'trumbull': 3734, 'tuscarawas': 3734, 'union': 3734, 'van wert': 3734, 'vinton': 3735, 'warren': 3735,
                'washington': 3735, 'wayne': 3734, 'williams': 3734, 'wood': 3734, 'wyandot': 3734}

    possible_crs = str()

    try:

        keyguess = toconvert.lower().rsplit("_")[0]
        value = str(newcodes[keyguess])
        possible_crs = "EPSG:" + value
        print(possible_crs)
    except IndexError:
        for key in newcodes.keys():
            print(key)
            if key in toconvert.lower():
                possible_crs = "EPSG:" + str(newcodes[key])
            # if possible_crs:
            #     possible_crs = "EPSG:" + possible_crs
                break

    return possible_crs


# Uploads a geo-file to the database.  The password for the database is stored in this
def uploadFunction(filepath: Path, user, password) -> None:

    # To not store the password as plain-text
    temp = tempfile.NamedTemporaryFile()
    pwrd: bytes = b'\xc3.\x04\XXXXXXXXXXXXXXXX
    temp.write(pwrd)
    temp.seek(0)

    try:
        sourceSrs = checkSRS(filepath)

        if sourceSrs != "":
            filename = filepath.name.rsplit(".")[0]

            if filepath.name.endswith(".zip"):
                unzip = "/vsizip/"
            else:
                unzip = ""

            signon = f"PG:\"dbname='defaultdb' sslmode='require' active_schema='public'" \
                     f" host='XXXXXXXXXXXXXXXXXXXXXXXXXXXX.com' port='25060' user='{user}'" \
                     f" password='{PGPMessage.from_file(temp.name).decrypt(passphrase=password).message}'\""

            command = f'ogr2ogr -f "PostgreSQL"  {signon} -progress --config PG_USE_COPY YES -overwrite -lco GEOMETRY_NAME=geom' \
                      f' -lco FID=id -lco DIM=2 -s_srs {sourceSrs} -t_srs EPSG:4326 -nln {filename.lower()} -nlt PROMOTE_TO_MULTI {unzip}{filepath}'

            popen(command).read()

        else:
            print(f"{filepath.name} cannot be uploaded without a projection")

    except Exception as e:
        print(f"{filepath.name} cannot be uploaded")
        print(str(e))
