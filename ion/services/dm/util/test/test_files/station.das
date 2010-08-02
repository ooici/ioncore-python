Attributes {
    NC_GLOBAL {
        String data_url "dods.ndbc.noaa.gov";
        String station "41001";
        String description "150 NM East of Cape HATTERAS";
        String data_provider "National Data Buoy Center";
        String location "34.68 N 72.66 W ";
        String data_quality "Automated QC checks with manual editing and comprehensive monthly QC";
        String conventions "COARDS";
    }
    DODS_EXTRA {
        String Unlimited_Dimension "time";
    }
    atmp {
        String units "degC";
        String long_name "AIR TEMPERATURE";
        Float64 missing_value 999;
    }
    avgpd {
        String units "seconds";
        String long_name "AVERAGE WAVE PERIOD";
        Float64 missing_value 99;
    }
    vis {
        String units "statute miles";
        String long_name "VISIBILITY";
        Float64 missing_value 99;
    }
    wdir {
        String units "degrees";
        String long_name "WIND DIRECTION";
        Int32 missing_value 999;
    }
    gust {
        String units "meters per second";
        String long_name "WIND GUST SPEED";
        Float64 missing_value 99;
    }
    lon {
        String units "degrees_east";
        String long_name "LONGITUDE";
    }
    wvhgt {
        String units "meters";
        String long_name "SIGNIFICANT WAVE HEIGHT";
        Float64 missing_value 99;
    }
    wtmp {
        String units "degC";
        String long_name "SEA SURFACE TEMPERATURE";
        Float64 missing_value 999;
    }
    dewpt {
        String units "degC";
        String long_name "DEWPOINT TEMPERATURE";
        Float64 missing_value 999;
    }
    mwdir {
        String units "degrees";
        String long_name "MEAN WAVE DIRECTION";
        Int32 missing_value 999;
    }
    dompd {
        String units "seconds";
        String long_name "DOMINANT WAVE PERIOD";
        Float64 missing_value 99;
    }
    wspd {
        String units "meters per second";
        String long_name "WIND SPEED";
        Float64 missing_value 99;
    }
    time {
        String units "seconds since 1970-01-01 00:00:00 UTC";
        String long_name "EPOCH TIME";
    }
    lat {
        String units "degrees_north";
        String long_name "LATITUDE";
    }
    baro {
        String units "hPa";
        String long_name "SEA LEVEL PRESSURE";
        Float64 missing_value 9999;
    }
}
