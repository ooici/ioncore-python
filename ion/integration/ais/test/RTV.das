Attributes {
    lat {
        String standard_name "latitude";
        String units "degrees_north";
    }
    lon {
        String standard_name "longitude";
        String units "degrees_east";
    }
    site_lat {
        String long_name "Contributing radar site latitudes";
        String standard_name "latitude";
        String units "degrees_north";
    }
    site_lon {
        String long_name "Contributing radar site longitudes";
        String standard_name "longitude";
        String units "degrees_east";
    }
    site_code {
        String long_name "Contributing radar site code";
        DODS {
            Int32 strlen 25;
            String dimName "nSites_maxStrlen";
        }
    }
    site_netCode {
        String long_name "Contributing radar site network affiliation code";
        DODS {
            Int32 strlen 25;
            String dimName "nSites_maxStrlen";
        }
    }
    procParams {
        String long_name "RTV processing parameters";
        String comment "
01) Maximum GDOP threshold
02) Maximum speed threshold (cm s-1)
03) Minimum number of sites required
04) Minimum number of radials required
05) Maximum angular gap to interpolate radial
    data over (degrees, 0 = no interpolation)
06) Maximum gap in range to interpolate radial
    data over (range-resolution, 0 = no interpolation)
07) Spatial search radius for radial solutions (km)";
    }
    time {
        String standard_name "time";
        String units "seconds since 1970-01-01";
        String calendar "gregorian";
    }
    u {
        String standard_name "surface_eastward_sea_water_velocity";
        String units "m s-1";
        Int16 _FillValue -32768;
        Float32 scale_factor 0.01;
        String ancillary_variables "DOPx";
    }
    v {
        String standard_name "surface_northward_sea_water_velocity";
        String units "m s-1";
        Int16 _FillValue -32768;
        Float32 scale_factor 0.01;
        String ancillary_variables "DOPy";
    }
    DOPx {
        String long_name "longitudinal dilution of precision";
        String comment "The longitudinal dilution of precision (DOPx) represents the
contribution of the radars' configuration geometry to
uncertainty in the eastward velocity estimate (u). DOPx is a
direct multiplier of the standard error in obtaining the
standard deviation for the eastward velocity estimate from the
least squares best fit. DOPx and DOPy are commonly used to
obtain the geometric dilution of precision
(GDOP = sqrt(DOPx^2 + DOPy^2)), a useful metric for filtering
errant velocities due to poor geometry.";
        Int16 _FillValue -32768;
        Float32 scale_factor 0.01;
    }
    DOPy {
        String long_name "latitudinal dilution of precision";
        String comment "The latitudinal dilution of precision (DOPy) represents the
contribution of the radars' configuration geometry to
uncertainty in the northward velocity estimate (v). DOPy is a
direct multiplier of the standard error in obtaining the
standard deviation for the northward velocity estimate from the
least squares best fit. DOPx and DOPy are commonly used to
obtain the geometric dilution of precision
(GDOP = sqrt(DOPx^2 + DOPy^2)), a useful metric for filtering
errant velocities due to poor geometry.";
        Int16 _FillValue -32768;
        Float32 scale_factor 0.01;
    }
    NC_GLOBAL {
        String netcdf_library_version "netcdf library version \"3.6.3\" of Aug 14 2008 22:45:23 $";
        String format_version "HFRNet_1.0.0";
        String product_version "HFRNet_1.1.05";
        String Conventions "CF-1.1";
        String title "Near-Real Time Surface Ocean Velocity";
        String institution "Scripps Institution of Oceanography";
        String source "Surface Ocean HF-Radar";
        String history "29-Jul-2011 15:35:36:  Filtered U and V by GDOP < 1.25
29-Jul-2011 15:35:36:  NetCDF file created";
        String references "Terrill, E. et al., 2006. Data Management and Real-time
Distribution in the HF-Radar National Network. Proceedings
of the MTS/IEEE Oceans 2006 Conference, Boston MA,
September 2006.";
        String creator_name "Mark Otero";
        String creator_email "motero@mpl.ucsd.edu";
        String creator_url "http://cordc.ucsd.edu/projects/mapping/";
        String summary "Surface ocean velocities estimated from HF-Radar are
representitive of the upper 0.3 - 2.5 meters of the
ocean.  The main objective of near-real time
processing is to produce the best product from
available data at the time of processing.  Radial
velocity measurements are obtained from individual
radar sites through the HF-Radar Network and
processed to create near-real time velocities
(RTVs)";
        Float32 geospatial_lat_min 21.73596;
        Float32 geospatial_lat_max 46.49442;
        Float32 geospatial_lon_min -97.88385;
        Float32 geospatial_lon_max -57.23121;
        String grid_resolution "6km";
        String grid_projection "equidistant cylindrical";
        String regional_description "Unites States, East and Gulf Coast";
    }
}
