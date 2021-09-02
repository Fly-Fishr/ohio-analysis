import queue
from geopandas import GeoDataFrame
from geoutilities import convert
import geopandas
import rasterstats
import pandas
import multiprocessing
import threading
from pathlib import Path
import getpass
import tempfile
from pgpy import PGPMessage

import logging

                        ##### TO DO #####
# The CRS formats need to be more streamlined and proper checking should be done to verify the county (overlaps its namesake
# in the Ohio counties vector file). Also, to_crs should be switched to in-place where appropriate eliminating unnecessary changes.
# The slope and aspect rasters need transformed to ESRI:54012 to be equal-area.  Equal-area needs to be used in the acreage and distance
# calculations as well.

ohioSlope = Path("../data/ohioSlope.tif")
ohioAspect = Path("../data/ohioAspect.tif")
electricAvenues = Path("../data/electricAvenues.gpkg")
bufferedOhio = Path("../data/bufferedOHcounties.gpkg")


# Drops invalid and null geometries.  Then call() sends parcels, filename, slopeFile, and aspectFile to generateTasks()
# to create the threadQueue, which is then passed to run(parcels, threadQueue).  Once the parcels are processed,
# call() returns parcels to where it was called from (either by the user directly or from the samePool() function.
def call(parcels, filename: str, slopeFile=ohioSlope, aspectFile=ohioAspect, togrid=False, save=False) -> GeoDataFrame:

    if isinstance(parcels, str) or isinstance(parcels, Path):
        parcels = geopandas.read_file(parcels)

    # Will merge sections of a polygon that overlap or touches itself in a way that makes it an invalid geometry
    # (e.g. infinite points along a shared edge)
    parcels.loc[:, "geometry"] = parcels.buffer(0)
    parcels.loc[:, "orig_id"] = pandas.Series(parcels.index.values + 1).values
    parcels = parcels.loc[(((~parcels.is_empty) & (parcels.geometry.notna())) & (parcels.is_valid))].copy()

    threadQueue = generateTasks(parcels, filename, slopeFile, aspectFile, togrid)

    processed = run(parcels, threadQueue)
    processed = processed[processed.slope.notna() & processed.aspect.notna()].reset_index()


    if save:
        try:
            output = str(Path(f"{save}/{processed.county[0].lower()}_parcels.gpkg"))
            processed.to_file(output, driver="GPKG")
        except:
            print("unable to save")

    return processed


# Generates a thread queue to keep the geofile's (parcels) processing moving in the correct order and not timing out (the slope
# and aspect calculations do not work well when both are operating on the same file).
# Queue order is convertToEqualArea -> formatTable -> convertToNAD83 -> calcSlope -> calcAspect
def generateTasks(parcels: GeoDataFrame, filename: str, slopeFile, aspectFile, togrid) -> queue.Queue:
    threadQueue = queue.Queue()


    t2 = threading.Thread(target=s2convertToStatePlane, args=(parcels, filename,))
    t3 = threading.Thread(target=s3formatTable, args=(parcels, filename,))
    t4 = threading.Thread(target=s4convertToNAD83, args=(parcels,))
    t6 = threading.Thread(target=s6calcSlope, args=(parcels, slopeFile,))
    t7 = threading.Thread(target=s7calcAspect, args=(parcels, aspectFile,))

    if not togrid:
        threadList = [t2, t3, t4, t6, t7]
    else:
        t8 = threading.Thread(target=distance, args=(parcels, electricAvenues, "to_grid",))
        threadList = [t2, t3, t4, t6, t7, t8]

    for item in threadList:
        threadQueue.put(item)

    return threadQueue


# Works through the thread queue to process the parcele.  Drops any parcels whose slope or aspects could not be calculated.
def run(parcels, threadQueue: queue.Queue) -> GeoDataFrame:
    q = threadQueue

    while not q.empty():
        t = q.get()
        t.start()
        t.join()
        q.task_done()

    return parcels.loc[:, ["orig_id", "owner", "address", "geometry", "county", "to_grid", "acres", "slope", "aspect"]]


def s2convertToStatePlane(parcels: GeoDataFrame, filename: str) -> None:
    epsg = convert.searchForCode(filename.rsplit("_")[0]).rsplit(":")[1]

    if not parcels.crs:
        try:
            parcels.set_crs(epsg=epsg, inplace=True)
        except Exception as e:
            print(str(e))
    else:
        parcels.to_crs(epsg=epsg, inplace=True)




# on very large data sets with lot of columns, there is an error in (possibly) NumPy package that requires the panda.series.values
def s3formatTable(parcels: GeoDataFrame, filename: str) -> None:
    county = filename.rsplit("_")[0]
    parcels.columns = parcels.columns.str.lower()
    parcels.loc[:, "acres"] = pandas.DataFrame(round((parcels.geometry.area / 43560), 2)).values
    parcels.loc[:, "county"] = county.upper()
    parcels.loc[:, "slope"] = pandas.NA
    parcels.loc[:, "aspect"] = pandas.NA
    parcels.loc[:, "to_grid"] = pandas.NA

    if "owner" not in parcels.columns:
        parcels.loc[:, "owner"] = ""
    if "address" not in parcels.columns:
        parcels.loc[:, "address"] = ""


# This is the CRS of the the raster slope and aspect images
# The raster CRS needs to eventually be transformed to Eckert IV
def s4convertToNAD83(parcels: GeoDataFrame) -> None:

    parcels.to_crs(epsg=4269, inplace=True)



# calculate slope
def s6calcSlope(parcels: GeoDataFrame, slopeFile=ohioSlope) -> None:

    parcels.loc[:, "slope"] = geopandas.GeoDataFrame(rasterstats.zonal_stats(parcels, str(slopeFile), stats="mean"))["mean"].values



# calculate aspect
def s7calcAspect(parcels: GeoDataFrame, aspectFile=ohioAspect) -> None:

    parcels.loc[:, "aspect"] = geopandas.GeoDataFrame(rasterstats.zonal_stats(parcels, str(aspectFile), stats="mean"))["mean"].values



# Will create a process pool to calculate the slope, aspect, acreage, and distance to grid.
# It will accept a string, path, list of paths, or a GeoDataFrame.
# Saving the file or uploading the file to an sql database is optional.
# It is also optioanl to return a GeoDataFrame.
# The number of used processor cores is selectable or will default to (total # of cores)-1
def samePool(parcelList, slopeFile=ohioSlope, aspectFile=ohioAspect, togrid=False, save=False, masterfile=False, upload=False, cores=None, county=""):

    if cores is None:
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()-1)
    else:
        pool = multiprocessing.Pool(processes=cores)
    master = geopandas.GeoDataFrame()
    results = list()

    if upload:
        password = getpass.getpass(prompt="PASSPHRASE PLEASE:")
        temp = tempfile.NamedTemporaryFile()
        pwrd: bytes = b'\xXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxx'
        temp.write(pwrd)
        temp.seek(0)

    if isinstance(parcelList, list):
        for p in parcelList:
            try:
                parcels = geopandas.read_file(p)
                results.append(pool.apply_async(call, args=(parcels, p.name, slopeFile, aspectFile, togrid, save)))
            except Exception as e:
                print(e)

    elif isinstance(parcelList, str) or isinstance(parcelList, Path):
        try:
            geofile = geopandas.read_file(parcelList)
            results.append(pool.apply_async(call, args=(geofile, Path(parcelList).name, slopeFile, aspectFile, togrid)))
        except Exception as e:
            print(e)

    elif isinstance(parcelList, GeoDataFrame):
        geodf = parcelList.copy()
        try:
            filename = geodf.county[geodf.first_valid_index()] + "_parcels"
        except:
            try:
                filename = county

            except:
                filename = input("What is the county?")

        results.append(pool.apply_async(call, args=(geodf, filename, slopeFile, aspectFile, togrid)))

    pool.close()
    pool.join()

    if upload:
        import sqlalchemy
        try:
            db = f"postgresql://doadmin:{PGPMessage.from_file(temp.name).decrypt(passphrase=password).message}@XXXXXXXXXXXXXXXXXXXXx.com:25060/defaultdb"
            con = sqlalchemy.create_engine(db)
        except:
            print("Unable to upload")

    for dataframe in results:
        gdf = geopandas.GeoDataFrame(dataframe.get())

        if masterfile:
            master = master.append(gdf)
        if upload:
            try:
                gdf.to_crs(epsg=4326).to_postgis(f"{gdf.county[0].lower()}_parcels", con=con, if_exists="replace", chunksize=20000)
                print(f"{gdf.county[0].lower()} UPLOADED")
            except:
                print(f"{gdf.county[0].lower()}_parcels error")
        if save:
            try:
                output = str(Path(f"{save}/{gdf.county[0].lower()}_parcels.gpkg"))
                gdf.to_file(output, driver="GPKG")
                print(f"{gdf.county[0].lower()} saved")
            except Exception as e:
                print("SAVE ERROR")
                print(e)


    if masterfile:
        # master.set_crs(epsg=4269, inplace=True)
        return master


# Calculates the minimum distance of each feature in a one collection to the closest feature in another.
# In our case, it will be a section of land parcels and a electric grid line or substation.
# The default is a collection of both transmission lines and substations.
def distance(section, whereto=electricAvenues, columnname="to_grid", buffZone=bufferedOhio, save=False):

    if isinstance(whereto, str) or isinstance(whereto, Path):
        whereto = geopandas.read_file(whereto)

    if buffZone:
        try:
            if isinstance(buffZone, str) or isinstance(buffZone, Path):
                buffered = geopandas.read_file(buffZone)
            else:
                print("buffzone else")
                buffered = buffZone

            # section.county -> all string elements in the series are the same so pick the first
            mask = buffered[buffered.COUNTY == section.county[section.first_valid_index()]]
            whereto = geopandas.clip(whereto, mask.to_crs(whereto.crs))

        except Exception as e:
            print("BUFFZONE ERROR")
            print(e)


    # The distance from each feature in 'section' to the closest [electric grid]feature in 'whereto'
    section.loc[:, columnname] = section.to_crs(whereto.crs)["geometry"].apply(lambda x: whereto.distance(x).min())
    if save:
        output = str(Path(f"{save}/{section.county[0].lower()}_parcels.gpkg"))
        section.to_file(output, driver="GPKG")

    return section


# If desired, a single dataframe will be divided into sections to calculate the 'to grid' information and use multiple processors to improve speed
def distanceparallel(whole, whereto=electricAvenues, columnname="to_grid", save=False, buffZone=False):
    import geopandas as gpd
    pool = multiprocessing.Pool(processes=2)

    if isinstance(whole, str) or isinstance(whole, Path):
        whole = geopandas.read_file(whole)

    groups = multiprocessing.cpu_count()
    divided = [whole[i::groups] for i in range(groups)]
    results = list()



    if buffZone:
        try:
            if isinstance(buffZone, str) or isinstance(buffZone, Path):
                buffZone = geopandas.read_file(buffZone)
        except Exception as e:
            print(e)


    for section in divided:
        if not buffZone:
            results.append(pool.apply_async(distance, args=(section, whereto, columnname)))
        else:
            results.append(pool.apply_async(distance, args=(section, whereto, columnname, buffZone)))


    pool.close()
    pool.join()

    calcdDistances = gpd.GeoDataFrame()
    for frame in results:
        gdataframe = frame.get()
        calcdDistances = calcdDistances.append(gdataframe)

    if save:
        output = str(Path(f"{save}/{calcdDistances.county[0].lower()}_parcels.gpkg"))
        calcdDistances.to_file(output, driver="GPKG")

    return calcdDistances[columnname]


