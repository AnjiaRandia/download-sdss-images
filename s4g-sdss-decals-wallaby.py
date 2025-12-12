#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
download sdss data from topcat's sia tables
"""
import pandas as pd
import requests
from astroquery.cadc import Cadc
import astropy.units as u

path = "/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/images/sdss-sia/" # from topcat's SIA

r = pd.read_csv(path+"sia-r.csv")
i = pd.read_csv(path+"sia-i.csv")
g = pd.read_csv(path+"sia-g.csv")
#u = pd.read_csv(path+"sia-u.csv")
z = pd.read_csv(path+"sia-z.csv")

all_s4g = pd.read_csv("/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/sample_catalog/start_leda/S4G_match_clean.csv")

to_decals = all_s4g[~all_s4g["object"].isin(i["object"])]  #decals data for the ones not in sdss

to_decals[["object","ra","dec"]].to_csv("/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/sample_catalog/start_leda/S4G_match_to-upload.csv")
to_decals.to_csv("/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/sample_catalog/start_leda/todecals.csv")
def download(df_row):
    for _,galaxy in df_row.iterrows():
        url = galaxy["URL"]
        name = galaxy["object"]
        survey = galaxy["Survey"]
        band = survey[-1]
        print(f"\ndownloading {name}, band: {band}")
        
        destination = path + name + f"/{name}_{band}.fits"
        r = requests.get(url)
        with open(destination, "wb") as f:
            f.write(r.content)
        print("done\n")
        
def bat_file(row):
    for _, galaxy in row.iterrows():
        url = galaxy["URL"]
        name = galaxy["object"]
        survey = galaxy["Survey"]
        band = survey[-1]
        destination = path + name + f"/{name}_{band}.fits"
        
        with open(path+"sdss_download.bat", "a") as f:
            f.write(f"echo '{name}:'\n\nwget -c -O '{destination}' '{url}'\n")
            
def get_jpg(row):
    # to download the jpgs of images to inspect sdss data
    # ofc you need to delet the .bat file when re running
    for _, galaxy in row.iterrows():
            ra, dec = galaxy["ra"], galaxy["dec"]
            name = galaxy["object"]
            url = f"https://skyserver.sdss.org/dr19/SkyServerWS/ImgCutout/getjpeg?ra={ra}&dec={dec}&scale=0.2&height=512&width=512&opt=G"
            target = "/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/images/sdss/jpg_cutouts/"
            target = "/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/plots/todelet/"
            
            with open(target+"download_cutouts.bat","a") as f:
                f.write(f"\n\nwget -O '{name}.jpg' '{url}'\n")

def get_jpg_decals(row):
    # to download the jpgs of images to inspect sdss data
    # ofc you need to delet the .bat file when re running
    for _, galaxy in row.iterrows():
            ra, dec = galaxy["ra"], galaxy["dec"]
            name = galaxy["object"]
            
            d25 = 10**(galaxy["logd25"]) * .1 * 60 # from d25 in log.1arcmin to arcsec
            pixscale = d25 * 1.25 / 512 #so that the whole galaxy fits inside the image

            url = f"https://www.legacysurvey.or/viewer/jpeg-cutout?ra={ra}&dec={dec}&layer=ls-dr10&pixscale={pixscale}"
            target = "/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/images/decals/jpg_cutouts/"

            
            with open(target+"download_cutouts.bat","a") as f:
                f.write(f"\n\nwget -O '{name}.jpg' '{url}'\n")
                
def mkdir(obj_table):
    #create a sh file to add mkdir to create the galaxy directories
    for names in obj_table["object"]:
        with open("/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/images/decals/list.sh", "a") as mkdir:
            mkdir.write(" "+names)

def get_data_decals(row):
    # to download the jpgs of images to inspect sdss data
    # ofc you need to delet the .bat file when re running
    bands = ["g","r","i","z"]
    for _, galaxy in row.iterrows():
            ra, dec = galaxy["ra"], galaxy["dec"]
            name = galaxy["object"]
            
            d25 = 10**(galaxy["logd25"]) * .1 * 60# from d25 in log.1arcmin to arcsec
            pixscale = d25 * 1.25 / 512 #so that the whole galaxy fits inside the image
            target = "/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/images/decals/"
            
            for b in bands:
                url = f"https://www.legacysurvey.org/viewer/fits-cutout?ra={ra}&dec={dec}&layer=ls-dr10&pixscale={pixscale}&bands={b}"
                destination = target + name + f"/{name}_{b}.fits"
                
                with open(target+"decals_download.bat","a") as f:
                    f.write(f"\necho '{name}, band: {b}'\nwget -c -O '{destination}' '{url}'\n\n")


def s4g_bat(path):
    # input is path to the bat file
    with open(path) as f:
        lines = f.readlines()
    with open(path+".bat","w") as f:
        for line in lines:
            if "1.final_mask.fits" in line.strip() or ".phot.1.fits" in line.strip():
                f.write(line)
                
def get_link_cadc(catalog):
    cadc = Cadc()
    wallaby_bat_path = "/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/images/wallaby/WALLABY/wallaby_download.bat"
    for _, row in catalog.iterrows():
        print(row['"Obs. ID"'],row['"RA (J2000.0)"'],row['"Dec. (J2000.0)"'])
        coords = str(row['"RA (J2000.0)"'])+' '+str(row['"Dec. (J2000.0)"'])
    
        region_query = cadc.query_region_async(coords,
                                               radius = 1*u.arcsec,
                                               collection = "WALLABY"
                                               )
        condition = (
                     row['"Product ID"'] == region_query['productID']
                     )

        urls = cadc.get_data_urls(region_query[condition],include_auxiliaries=False)
        mom0_url = [url for url in urls if "mom0.fits" in url]
        print(mom0_url)
    
        with open(wallaby_bat_path, "a") as batfile:
            batfile.write('wget -x "'+mom0_url[0]+'"\n')
#get_jpg(i)
get_jpg(to_decals)
#get_jpg_decals(to_decals)
#bat_file(r), bat_file(g), bat_file(u), bat_file(z), bat_file(i)
#mkdir(to_decals)
#download(u)
#get_data_decals(to_decals)

# edit the bat files containing all the wget to get s4g data:
#s4g_bat("/mnt/364E6D864E6D3FAB/Files/data/bar_ellipse_tofit/images/s4g/irsa.ipac.caltech.edu/download_all.bat")

# download the rest of wallaby data (with a bat file again)

#get_link_cadc(to_decals)
