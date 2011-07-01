Attributes {
    time {
        String standard_name "time";
        String long_name "time";
        String units "days since 1950-01-01T00:00:00Z";
        String FORTRAN_format "F16.8";
        Float64 valid_min 0.0;
        Float64 valid_max 90000.0;
        Int16 QC_indicator 1;
        String QC_value "Good data";
        Int16 QC_procedure 1;
        String QC_procedure_value "Ranges applied";
        String axis "T";
    }
    depth {
        String long_name "measurement depth";
        String standard_name "depth";
        String units "meters";
        String FORTRAN_format "F10.3";
        String positive "down";
        String axis "Z";
        String coordinate_reference_frame "urn:ogc:crs:EPSG::5113";
        Float64 valid_min -2.0;
        Float64 valid_max 2.0;
        Int16 QC_indicator 7;
        String QC_value "Nominal value ";
        Int16 QC_procedure 0;
        String QC_procedure_value "No QC performed ";
        String comment "surface data; nominal depth == 0";
    }
    latitude {
        String long_name "latitude";
        String standard_name "latitude";
        String units "degrees_north";
        String FORTRAN_format "F10.4";
        Float32 _FillValue 1.0E35;
        Float64 valid_min -90.0;
        Float64 valid_max 90.0;
        String axis "Y";
        String reference "WGS84";
        String coordinate_reference_frame "urn:ogc:crs:EPSG::4326";
        Int16 QC_indicator 7;
        String QC_value "Nominal value ";
        Int16 QC_procedure 0;
        String QC_procedure_value "No QC performed";
        String comment "nominal position";
    }
    longitude {
        String long_name "longitude";
        String standard_name "longitude";
        String units "degrees_east";
        String FORTRAN_format "F10.4";
        Float32 _FillValue 1.0E35;
        Float64 valid_min -180.0;
        Float64 valid_max 180.0;
        String axis "X";
        String reference "WGS84";
        String coordinate_reference_frame "urn:ogc:crs:EPSG::4326";
        Int16 QC_indicator 7;
        String QC_value "Nominal value ";
        Int16 QC_procedure 0;
        String QC_procedure_value "No QC performed";
        String comment "nominal position";
    }
    AIRT {
        String standard_name "air_temperature";
        String long_name "Air Temperature";
        String units "degree_C";
        String FORTRAN_format "f8.2";
        Float32 _FillValue 1.0E35;
        Float32 valid_min 21.0;
        Float32 valid_max 29.0;
        String SN "211";
        String model "ASIMet_HRH";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    ATMS {
        String standard_name "air_pressure_at_sea_level";
        String long_name "barometric pressure";
        String units "millibars";
        String FORTRAN_format "f12.2";
        Float32 _FillValue 1.0E35;
        Float32 valid_min 1007.0;
        Float32 valid_max 1019.0;
        String SN "224";
        String model "ASIMet_BPR";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    RELH {
        String standard_name "relative_humidity";
        String long_name "relative humidity";
        String units "percent";
        String FORTRAN_format "f8.2";
        Float32 _FillValue 1.0E35;
        Float32 valid_min 52.0;
        Float32 valid_max 93.0;
        String SN "211";
        String model "ASIMet_HRH";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    LW {
        String standard_name "surface_downwelling_longwave_flux_in_air ";
        String long_name "longwave radiation";
        String units "W m-2";
        String FORTRAN_format "7.2";
        Float32 _FillValue 1.0E35;
        Float32 valid_min 353.0;
        Float32 valid_max 452.0;
        String SN "209";
        String model "ASIMet_LWR";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    RAIT {
        String standard_name "thickness_of_rainfall_amount";
        String long_name "precipitation thickness";
        String units "mm";
        String FORTRAN_format "f6.1";
        Float32 _FillValue 1.0E35;
        Float32 valid_min 0.0;
        Float32 valid_max 51.0;
        String SN "216";
        String model "ASIMet_PRC";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    TEMP {
        String standard_name "sea_water_temperature";
        String long_name "sea temperature";
        String units "degree_C";
        String FORTRAN_format "f12.4";
        Float32 _FillValue 1.0E35;
        String comment "from .45m temp";
        Float32 valid_min 26.0;
        Float32 valid_max 30.0;
        String SN "1836";
        String model "SBE_37";
        Float32 sensor_height -100.0;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    SW {
        String standard_name "surface_downwelling_shortwave_flux_in_air";
        String long_name "shortwave radiation";
        String units "W m-2";
        String FORTRAN_format "f7.2";
        Float32 _FillValue 1.0E35;
        Float32 valid_min 2.0;
        Float32 valid_max 1050.0;
        String SN "226";
        String model "ASIMet_SWR";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    UWND {
        String standard_name "eastward_wind";
        String long_name "wind_velocity_east";
        String units "meters/second";
        String FORTRAN_format "f12.2";
        Float32 _FillValue 1.0E35;
        Float32 valid_min -12.0;
        Float32 valid_max 4.0;
        Float32 magvar_applied_degrees NaN;
        String SN "225";
        String model "ASIMet_SWND";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    VWND {
        String standard_name "northward_wind";
        String long_name "wind velocity north";
        String units "meters/second";
        String FORTRAN_format "f12.2";
        Float32 _FillValue 1.0E35;
        Float32 valid_min -11.0;
        Float32 valid_max 6.0;
        Float32 magvar_applied_degrees NaN;
        String SN "225";
        String model "ASIMet_SWND";
        Float32 sensor_height NaN;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    PSAL {
        String standard_name "sea_water_salinity";
        String long_name "salinity";
        String units "1";
        String FORTRAN_format "f8.4";
        Float32 _FillValue 1.0E35;
        Float32 valid_min 28.0;
        Float32 valid_max 37.0;
        String equation_of_state "EOS-80";
        String SN "1836";
        String model "SBE_37";
        Float32 sensor_height -100.0;
        String cell_methods "TIME:mean depth: point latitude: point longitude: point";
        Int16 QC_indicator 2;
        Int16 QC_procedure 1;
    }
    NC_GLOBAL {
        String data_type "OceanSites time-series data";
        String Conventions "CF-1.4";
        String standard_name_vocabulary "CF 1.4";
        String Metadata_Conventions "Unidata Dataset Discovery v1.0";
        String netcdf_version "3.5";
        String format_version "1.2";
        String institution "WHOI";
        String source "Mooring observation";
        String naming_authority "OceanSITES";
        String cdm_data_type "Station";
        String data_assembly_center "WHOI-UOP";
        String distribution_statement " Follows CLIVAR (Climate Varibility and Predictability) standards, cf. http://www.clivar.org/data/data_policy.php. Data available free of charge. User assumes all risk for use of data. User must display citation in any publication or product using data. User must contact PI prior to any commercial use of data.";
        String institution_references "http://uop.whoi.edu";
        String publisher_name "Nan Galbraith";
        String publisher_url "http://uop.whoi.edu";
        String publisher_email "ngalbraith@whoi.edu";
        String site_code "NTAS";
        String title "NTAS 10 Real-time Mooring Data, System 1";
        String platform_code "NTAS";
        String wmo_platform_code "48401";
        String deployment_number "10";
        String deployment_code "2010";
        String id "OS_NTAS_2010_R_M-1";
        String summary "Real-time surface data from ASIMet system 1 on the tenth Northwest Tropcial Stlantic Station (NTAS) observatory.";
        String pi_name "Albert Plueddemann";
        String author "Nan Galbraith";
        String contact "ngalbraith@whoi.edu";
        String creator_name "Nan Galbraith";
        String creator_email "ngalbraith@whoi.edu";
        String project "NTAS";
        String creator_url "http://uop.whoi.edu";
        String contributor_name "Albert Plueddemann";
        String contributor_role "Principal Investigator";
        String date_created "2011-06-30T08:23:49Z";
        String date_issued "2011-06-30T10:00:00Z";
        String date_update "20110630T082117";
        String date_modified "20110630T082117";
        String data_mode "R";
        String update_interval "P3H";
        String references "http:// www.oceansites.org, http://uop.whoi.edu/projects/ntas";
        String citation "Data provided by the Upper Ocean Processes Group of the Woods Hole Oceanographic Institution with support from the NOAA Office of Climate Observations";
        String acknowledgment "Data provided by the Upper Ocean Processes Group of the Woods Hole Oceanographic Institution with support from the NOAA Office of Climate Observations";
        String license "Follows CLIVAR Data Policy";
        String history "Thu Jun 30 08:35:01 2011: ncatted -O -a Conventions,global,o,c,CF-1.4 OS_NTAS_2010_R_M-1.nc OS_NTAS_2010_R_M-1.nc.tmp
20110630T082117 Argos data processed. N.Galbraith";
        String comment "Argos, hourly averaged ASIMet data";
        String qc_manual "http://www.oceansites.org/data/quality_control_manual.pdf";
        String quality_control_procedure "Ranges applied, bad data flagged";
        String quality_index "B";
        Int16 qc_flag_values 0, 1, 2, 3, 4;
        String qc_flag_meanings "no_qc_performed good probably_good correctable bad";
        String processing_level "minimal real-time qc performed";
        String area "Northwest Tropical Atlantic";
        String geospatial_lat_min "15";
        String geospatial_lat_max "15";
        String geospatial_lon_min "-51";
        String geospatial_lon_max "-51";
        String water_depth "�m";
        String geospatial_vertical_min "-1";
        String geospatial_vertical_max "2";
        String time_coverage_start "2010-12-10T04:00:00Z";
        String time_coverage_end "2011-06-30T10:00:00Z";
        String time_coverage_duration "202.25D";
        String keywords_vocabulary "GCMD Science Keywords";
        String keywords_vocabulary_URL "http://vocab.ndg.nerc.ac.uk/list/P131/4";
        String keywords "Ocean Winds, Atmospheric Radiation, Air Temperature, Salinity/Density,Ocean Temperature";
        String Instrument_type "ASIMet Logger";
        String Instrument_reference "http://uop.whoi.edu/asimet";
        String SN "12";
        String Metadata_Link "http://uop.whoi.edu/OceanSITES/OOI/OS_NTAS_2010_R_M-1.cdl";
        Int32 FILL_FLAG 0;
    }
    DODS_EXTRA {
        String Unlimited_Dimension "time";
    }
}
