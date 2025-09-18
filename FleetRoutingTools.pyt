# -*- coding: utf-8 -*-
"""Script tool to generates polygons based on existing route lines or parcel data, replacing existing geometry within
the Routes sublayer of the Waste Collection layer.

Copyright 2025 Esri
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
import traceback
import pandas as pd
import arcpy


DELETE_INTERMEDIATE_OUTPUTS = True  # Set to False for debugging
SCRATCH_GDB = "memory"


class Toolbox:
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Fleet Routing Tools"
        self.alias = "FleetRoutingTools"

        # List of tool classes associated with this toolbox
        self.tools = [GenerateRouteGeometryforWasteCollection]


class GenerateRouteGeometryforWasteCollection:
    """Tool definition for Generate Route Geometry for Waste Collection."""

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Generate Route Geometry for Waste Collection"
        self.description = ""
        self.scratch_gdb = SCRATCH_GDB
        self.temp_outputs = []

    def getParameterInfo(self):
        """Define the tool parameters."""
        param_nalayer = arcpy.Parameter(
            displayName="Waste Collection Layer",
            name="wc_layer",
            datatype="GPNALayer",
            parameterType="Required",
            direction="Input"
        )
        param_use_parcels = arcpy.Parameter(
            displayName="Use parcels",
            name="use_parcels",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        param_parcel_data = arcpy.Parameter(
            displayName="Parcel Data",
            name="parcel_data",
            datatype="GPFeatureLayer",
            parameterType="Optional",
            direction="Input",
        )
        param_parcel_data.filter.list = ["Polygon"]
        param_parcel_id = arcpy.Parameter(
            displayName="Parcel Unique ID Field",
            name="parcel_id",
            datatype="Field",
            parameterType="Optional",
            direction="Input"
        )
        param_parcel_id.parameterDependencies = [param_parcel_data.name]
        param_stops_id = arcpy.Parameter(
            displayName="Stops Parcel ID Field",
            name="stops_id",
            datatype="GPString",
            parameterType="Optional",
            direction="Input"
        )
        param_stops_id.filter.type = "ValueList"
        param_stops_id.filter.list = []

        return [
            param_nalayer,
            param_use_parcels,
            param_parcel_data,
            param_parcel_id,
            param_stops_id
        ]

    def isLicensed(self):
        """Set whether the tool is licensed to execute."""

        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        [
            param_nalayer,
            param_use_parcels,
            param_parcel_data,
            param_parcel_id,
            param_stops_id,
        ] = parameters
        if param_use_parcels.value:
            param_parcel_data.enabled = True
            param_parcel_id.enabled = True
            param_stops_id.enabled = True
            if (
                (param_stops_id.filter.list == [] or (not param_nalayer.hasBeenValidated))
                and param_nalayer.altered
                and param_nalayer.valueAsText
            ):
                try:
                    waste_layer = param_nalayer.value
                    if isinstance(waste_layer, str) and waste_layer.endswith(".lyrx"):
                        waste_layer = arcpy.mp.LayerFile(waste_layer).listLayers()[0]
                    stops_sublayer = arcpy.na.GetNASublayer(waste_layer, "Stops")
                    stops_field_names = [f.name for f in arcpy.ListFields(stops_sublayer)]
                    param_stops_id.filter.list = stops_field_names
                except Exception:  # pylint: disable=broad-except
                    # If this errors for some reason, just ignore it
                    param_stops_id.filter.list = []
        else:
            param_parcel_data.enabled = False
            param_parcel_id.enabled = False
            param_stops_id.enabled = False
            param_stops_id.filter.list = []
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter. This method is called after internal validation."""
        param_nalayer = parameters[0]
        param_use_parcels = parameters[1]

        # Version check
        arcgis_version = arcpy.GetInstallInfo()["Version"]
        if arcgis_version < "3.5":
            param_nalayer.setErrorMessage("This tool requires ArcGIS Pro 3.5 or higher.")

        # Validate the input layer
        if param_nalayer.altered and param_nalayer.valueAsText:
            waste_layer = param_nalayer.value
            if isinstance(waste_layer, str) and waste_layer.endswith(".lyrx"):
                waste_layer = arcpy.mp.LayerFile(waste_layer).listLayers()[0]

            # Make sure it's a Waste Collection layer
            try:
                desc = arcpy.Describe(waste_layer)
                solvername = desc.solverName
                if solvername != "Waste Collection Solver":
                    param_nalayer.setErrorMessage(
                        "The input layer is not a Waste Collection layer."
                    )
            except Exception:  # pylint: disable=broad-except
                param_nalayer.setErrorMessage("The input layer is not a valid network analysis Layer.")

            # Make sure relevant sublayers don't have joins
            if not param_nalayer.hasError():
                try:
                    stops_sublayer = arcpy.na.GetNASublayer(waste_layer, "Stops")
                    if "connection_info" not in stops_sublayer.connectionProperties.keys():
                        param_nalayer.setErrorMessage(
                            "Please remove all joins from the Stops sublayer before running this tool."
                        )
                    routes_sublayer = arcpy.na.GetNASublayer(waste_layer, "Routes")
                    if "connection_info" not in routes_sublayer.connectionProperties.keys():
                        param_nalayer.setErrorMessage(
                            "Please remove all joins from the Routes sublayer before running this tool."
                        )
                except Exception as ex:  # pylint: disable=broad-except
                    if arcgis_version < "3.6" and "naclass_name" in str(ex):
                        # This is a workaround for 3.5, when GetNASublayer didn't work if a
                        # sublayer had a join on it.  This problem was fixed in 3.6.
                        param_nalayer.setErrorMessage(
                            "Please remove all joins from the sublayers before running this tool."
                        )
                    else:
                        param_nalayer.setErrorMessage("The input layer is not a valid network analysis Layer.")

        # Make parcel parameters conditionally required.
        # Error 735 causes the little red require star to show up on the parameters
        if param_use_parcels.value is True:
            if not parameters[2].valueAsText:
                parameters[2].setIDMessage("Error", 735, parameters[2].displayName)
            if not parameters[3].valueAsText:
                parameters[3].setIDMessage("Error", 735, parameters[3].displayName)
            if not parameters[4].valueAsText:
                parameters[4].setIDMessage("Error", 735, parameters[4].displayName)
        else:
            parameters[2].clearMessage()
            parameters[3].clearMessage()
            parameters[4].clearMessage()
            if arcpy.ProductInfo() != "ArcInfo":
                param_use_parcels.setErrorMessage(
                    "Generating route geometry without parcel data requires an ArcGIS Pro Advanced license."
                    )

        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        geom_generator = None
        try:
            [
                param_nalayer,
                param_use_parcels,
                param_parcel_data,
                param_parcel_id,
                param_stops_id,
            ] = parameters
            waste_layer = param_nalayer.value
            # Set the progressor so the user is informed of progress
            arcpy.SetProgressor("default")
            if param_use_parcels.value:
                parcel_data = param_parcel_data.value
                parcel_id = param_parcel_id.valueAsText
                stops_id = param_stops_id.valueAsText
                geom_generator = RouteGeometryGeneratorFromParcels(
                    waste_layer, parcel_data, parcel_id, stops_id)
                geom_generator.create_route_geometry()
            else:
                geom_generator = RouteGeometryGeneratorFromStops(waste_layer)
                geom_generator.create_route_geometry()
        except RouteGeometryToolError:
            # Handle errors we've explicitly caught for known reasons
            # No need to do anything here since raising the error already added tool error messages
            pass
        except Exception:  # pylint: disable=broad-except
            # Handle unknown errors. Provide a traceback for debugging.
            arcpy.AddError("The tool failed for an unknown reason.")
            arcpy.AddError(traceback.format_exc())
        finally:
            # Clean up temporary outputs
            if geom_generator and DELETE_INTERMEDIATE_OUTPUTS:
                geom_generator.delete_intermediate_outputs()
        return


class RouteGeometryGenerator:
    """Parent class for generating route geometry for a Waste Collection layer."""

    def __init__(self, waste_layer):
        """Initialize the route geometry generator and do some basic checks of the Waste Collection layer."""
        self.temp_outputs = []
        self.scratch_gdb = SCRATCH_GDB
        self.temp_outputs = []
        self.waste_layer = waste_layer
        self.desc_waste_layer = arcpy.Describe(waste_layer)
        self.network = self._get_na_layer_network_dataset_path()
        self.stops_sublayer = arcpy.na.GetNASublayer(waste_layer, "Stops")
        self.routes_sublayer = arcpy.na.GetNASublayer(waste_layer, "Routes")
        self.route_lines_sublayer = arcpy.na.GetNASublayer(waste_layer, "RouteLines")
        # Do some basic checks to make sure the layer can be used
        if int(arcpy.management.GetCount(self.route_lines_sublayer).getOutput(0)) == 0:
            raise RouteGeometryToolError((
                "The RouteLines sublayer of the input Waste Collection layer is empty. "
                "Solve the layer before running this tool."
            ))
        if int(arcpy.management.GetCount(self.routes_sublayer).getOutput(0)) == 0:
            raise RouteGeometryToolError((
                "The Routes sublayer of the input Waste Collection layer is empty. "
                "Configure and solve the layer before running this tool."
            ))
        if int(arcpy.management.GetCount(self.stops_sublayer).getOutput(0)) == 0:
            raise RouteGeometryToolError((
                "The Stops sublayer of the input Waste Collection layer is empty. "
                "Configure and solve the layer before running this tool."
            ))

    def _make_temporary_output_path(self, name):
        """Make a path for a temporary intermediate output and track it for later deletion."""
        name = arcpy.ValidateTableName(name, self.scratch_gdb)
        temp_output = arcpy.CreateUniqueName(name, self.scratch_gdb)
        self.temp_outputs.append(temp_output)
        return temp_output

    def delete_intermediate_outputs(self):
        """Clean up temporary intermediate outputs."""
        if DELETE_INTERMEDIATE_OUTPUTS and self.temp_outputs:
            try:
                arcpy.management.Delete(self.temp_outputs)
                self.temp_outputs = []  # Clear the list after deletion
            except Exception:  # pylint: disable=broad-except
                pass

    def _get_na_layer_network_dataset_path(self):
        """Return clean network dataset path from the Waste Collection layer.

        If the layer references a mobile geodatabase network or a network in SDE, the Describe object's catalogPath
        property returns stuff like this:
        'AUTHENTICATION_MODE=OSA;main;DB_CONNECTION_PROPERTIES=C:\\Data\\commondata\\sanfrancisco.geodatabase;
        INSTANCE=sde:sqlite:C:\\Data\\commondata\\sanfrancisco.geodatabase;IS_GEODATABASE=true;IS_NOSERVER=0

        This method extracts the clean filepath of the network dataset from it.  The input is returned unchanged if the
        value doesn't match this format.
        """
        data_source = self.desc_waste_layer.network.catalogPath
        junk_prefix = "DB_CONNECTION_PROPERTIES="
        if junk_prefix in data_source:
            junk_parts = data_source.split(";")
            connection_path = [j for j in junk_parts if j.startswith(junk_prefix)]
            if connection_path:
                data_source = os.path.normpath(connection_path[0].split("=")[1])
                if data_source.startswith("\\"):
                    # For some reason UNC paths are not given starting with a double slash \\.
                    data_source = "\\" + data_source
                # At this point, the data source is the path to the geodatabase, and we need to add the feature dataset
                # and network dataset to the path
                layer_cim_def = self.waste_layer.getDefinition("V3")
                network_cim_def = layer_cim_def.networkDataset
                data_source = os.path.join(data_source, network_cim_def.featureDataset, network_cim_def.dataset)
        return data_source

    def _write_progress_msg(self, msg):
        """Write a progress message to the GP messages and the UI progressor."""
        arcpy.AddMessage(msg)
        arcpy.SetProgressorLabel(msg)

    def _make_field_mappings(self, source_table, fields_to_delete=None, fields_to_add=None):
        """Create a FieldMappings object for deleting and renaming fields in ExportFeatures."""
        field_mappings = arcpy.FieldMappings()
        # Initialize the field mappings using the default mappings from the original data
        field_mappings.addTable(source_table)
        # Remove undesired fields from the field mappings object. These fields will not show up in the output.
        fields_to_delete = fields_to_delete if fields_to_delete else []
        for field_name in fields_to_delete:
            field_idx = field_mappings.findFieldMapIndex(field_name)
            if field_idx != -1:  # Only remove it if it was there in the first place
                field_mappings.removeFieldMap(field_idx)
        # Add desired new fields
        fields_to_add = fields_to_add if fields_to_add else []
        for new_field_def in fields_to_add:
            # Create a new field object and set its properties
            new_field = arcpy.Field()
            for prop, value in new_field_def.items():
                if prop != "inputField":
                    setattr(new_field, prop, value)
            # Set this as an output field on the FieldMapping object
            new_fm = arcpy.FieldMap()
            new_fm.mergeRule = "First"
            new_fm.outputField = new_field
            if (input_field := new_field_def.get("inputField")):
                new_fm.addInputField(source_table, input_field)
            field_mappings.addFieldMap(new_fm)
        return field_mappings

    def _update_route_geometry(self, new_geom_fc):
        """Update the Routes sublayer feature geometry from the specified feature class."""
        self._write_progress_msg("Updating geometry of the Routes sublayer of the Waste Collection layer...")
        with arcpy.da.UpdateCursor(self.routes_sublayer, ["SHAPE@", "Name"]) as r_cur:  # pylint: disable=no-member
            for route_row in r_cur:
                route_name = route_row[1]
                new_geom = None  # Initialize to a null geometry value for the route
                # Replace with a new geometry object if there is one matching the RouteName
                for new_geom_row in arcpy.da.SearchCursor(  # pylint: disable=no-member
                    new_geom_fc, ["SHAPE@"], f"RouteName = '{route_name}'"
                ):
                    new_geom = new_geom_row[0]
                r_cur.updateRow([new_geom, route_name])


class RouteGeometryGeneratorFromStops(RouteGeometryGenerator):
    """Generate Waste Collection route geometries using information internal to the solved Waste Collection layer.

    In particular, generate polygons representing city blocks where Stops were located.  Identify the routes serving
    each block and subdivide the geometry of any block served by multiple routes using Thiessen polygons created around
    the Stops in that block.
    """

    def _create_blocks_from_network_edges(self):
        """Create block polygons using the network dataset edges that Stops got located on."""
        self._write_progress_msg("Creating blocks from network edges...")
        # Get some information about the Waste layer
        nds_obj = arcpy.nax.NetworkDataset(self.network)
        nds = nds_obj.describe()
        edge_source_names = [e.name for e in nds.edgeSources]
        nds_fd_path = os.path.dirname(self.network)

        # Create a dataframe associating edge source features with routes by using the Stops location fields
        # Skip any stops that are unlocated
        fields = ["RouteName", "SourceID", "SourceOID", "SideOfEdge"]
        with arcpy.da.SearchCursor(self.stops_sublayer, fields, "SourceOID <> -1") as cur:  # pylint: disable=no-member
            df = pd.DataFrame(cur, columns=fields)
        df.drop_duplicates(inplace=True)
        located_source_ids = list(df['SourceID'].unique())

        # Extract a subset of edge source features in close proximity to the analysis area to use for generating
        # polygon geometry
        located_edges_buffers = []
        for source_id in located_source_ids:
            edge_source = nds_obj.getDataSourceFromSourceID(int(source_id))
            # Make a subset layer with just the edge features that were located on
            source_oids = list(df[df['SourceID'] == source_id]['SourceOID'].unique())
            where = f"ObjectID IN ({', '.join([str(i) for i in source_oids])})"
            with arcpy.EnvManager(overwriteOutput=True):
                edge_source_lyr_used = arcpy.management.MakeFeatureLayer(edge_source, "EdgeSource", where)
            # Buffer it by our desired road setback
            # Note: The Pairwise Buffer tool doesn't expose the line_end_type option, so don't use it.
            # Note: The FLAT option is only available with the Advanced license. FLAT produces better results for our
            # needs, but ROUND is acceptable and available with the Basic license.  However, unless we find workarounds
            # for the other tools used in this method that require the Advanced license, it's not worth adding logic
            # here.
            located_edges_buffer = self._make_temporary_output_path(f"LocatedEdgesBuffer_SourceID{source_id}")
            arcpy.analysis.Buffer(
                edge_source_lyr_used,
                out_feature_class=located_edges_buffer,
                buffer_distance_or_field="30 Meters",
                line_end_type="FLAT",
                dissolve_option="ALL",
                method="PLANAR"
            )
            located_edges_buffers.append(located_edges_buffer)

        # When multiple edge sources were located on, combine all of them into one big buffer
        if len(located_edges_buffers) > 1:
            located_edges_buffer = self._make_temporary_output_path("LocatedEdgesBufferMerged")
            arcpy.management.Merge(
                located_edges_buffers,
                located_edges_buffer,
                add_source="NO_SOURCE_INFO"
            )
        else:
            located_edges_buffer = located_edges_buffers[0]

        # Eliminate interior holes in the buffer. Fully surrounded blocks will be filled in.
        located_edges_buffer2 = self._make_temporary_output_path("LocatedEdgesBufferUnion")
        arcpy.analysis.Union(
            located_edges_buffer,
            located_edges_buffer2,
            join_attributes="ONLY_FID",
            gaps="NO_GAPS"
        )
        located_edges_buffer3 = self._make_temporary_output_path("LocatedEdgesBufferFinal")
        arcpy.analysis.PairwiseDissolve(
            in_features=located_edges_buffer2,
            out_feature_class=located_edges_buffer3,
            multi_part="MULTI_PART"
        )

        # Select all edges from all edge sources that are within the buffer
        extracted_edges = []
        for edge_source_name in edge_source_names:
            edge_source = os.path.join(nds_fd_path, edge_source_name)
            with arcpy.EnvManager(overwriteOutput=True):
                edge_source_lyr_all = arcpy.management.MakeFeatureLayer(edge_source, "EdgeSourceAll")
            arcpy.management.SelectLayerByLocation(
                in_layer=edge_source_lyr_all,
                overlap_type="INTERSECT",
                select_features=located_edges_buffer3
            )
            # Extract the selected features without attributes
            field_mappings = self._make_field_mappings(
                edge_source,
                fields_to_delete=[f.name for f in arcpy.ListFields(edge_source)]
            )
            field_mappings = None
            clipped_edges = self._make_temporary_output_path(f"ClippedEdges_{edge_source_name}")
            arcpy.conversion.ExportFeatures(
                in_features=edge_source_lyr_all,
                out_features=clipped_edges,
                field_mapping=field_mappings
            )
            extracted_edges.append(clipped_edges)

        # Convert the buffer polygon to a polyline to use as a boundary and merge it with the extracted streets
        # Note: Tool requires the Advanced license
        bounding_box_polyline = self._make_temporary_output_path("bounding_box_polyline")
        arcpy.management.PolygonToLine(
            in_features=located_edges_buffer3,
            out_feature_class=bounding_box_polyline,
        )
        extracted_edges.append(bounding_box_polyline)
        clipped_bound_edges = self._make_temporary_output_path("clipped_bound_edges")
        arcpy.management.Merge(
            inputs=extracted_edges,
            output=clipped_bound_edges
        )

        # Create polygon block features from the clipped and bound streets
        # Note: Tool requires the Standard license (not Basic)
        blocks = self._make_temporary_output_path("Blocks")
        arcpy.management.FeatureToPolygon(clipped_bound_edges, blocks)

        return blocks

    def _associate_blocks_with_routes(self, blocks):
        """Associate block polygons with the routes that serve them."""
        self._write_progress_msg("Associating blocks with the routes that serve them...")
        # Make a dummy Waste Collection layer in a temporary gdb workspace
        travel_mode = self.waste_layer.getDefinition("V3").solver["appliedTravelModeJSON"]
        with arcpy.EnvManager(workspace=arcpy.env.scratchGDB):
            dummy_lyr = arcpy.na.MakeWasteCollectionAnalysisLayer(self.network, travel_mode=travel_mode).getOutput(0)
        # Add the real Waste Collection layer's stops to the dummy layer using network location fields and a snap offset
        stops_sublayer_name = arcpy.na.GetNAClassNames(dummy_lyr)["Stops"]
        field_mappings = arcpy.na.NAClassFieldMappings(
            dummy_lyr, stops_sublayer_name, True)
        arcpy.na.AddLocations(
            dummy_lyr,
            stops_sublayer_name,
            self.stops_sublayer,
            field_mappings,
            snap_to_position_along_network="SNAP",
            snap_offset="0.1 Meters",
        )

        # Extract the Stops sublayer with the stop geometry at their snapped locations for further use
        dummy_stops = self._make_temporary_output_path("DummyStops")
        arcpy.conversion.ExportFeatures(
            in_features=arcpy.na.GetNASublayer(dummy_lyr, "Stops"),
            out_features=dummy_stops
        )

        # Clean up the dummy Waste Collection layer
        try:
            arcpy.na.DeleteNetworkAnalysisLayer(dummy_lyr)
        except Exception:  # pylint: disable=broad-except
            # Ignore deletion failures
            pass

        # Use a spatial join between blocks and dummy stops to associate the blocks with the routes that serve them
        # This results in a table with a copy of each block for each stop that intersects it
        blocks_joined = self._make_temporary_output_path("BlocksJoined")
        field_mappings = self._make_field_mappings(
            dummy_stops,
            [f.name for f in arcpy.ListFields(dummy_stops) if f.name not in ["Name", "RouteName"]]
        )
        arcpy.analysis.SpatialJoin(
            target_features=blocks,
            join_features=dummy_stops,
            out_feature_class=blocks_joined,
            join_operation="JOIN_ONE_TO_MANY",
            join_type="KEEP_COMMON",
            field_mapping=field_mappings,
            match_option="INTERSECT"
        )

        # Create a dataframe to relate block OIDs with RouteNames and stop names
        with arcpy.da.SearchCursor(  # pylint: disable=no-member
            blocks_joined, ["TARGET_FID", "Name", "RouteName"]
        ) as cur:
            df = pd.DataFrame(cur, columns=["block_id", "StopName", "RouteName"])

        return df, dummy_stops

    def create_route_geometry(self):
        """Create route geometry for the Waste Collection layer using data internal to its solution."""
        # Create blocks from network edges
        blocks = self._create_blocks_from_network_edges()
        blocks_desc = arcpy.Describe(blocks)
        blocks_oid_field = blocks_desc.oidFieldName
        blocks_extent = blocks_desc.extent
        # Associate blocks with the routes that serve them
        block_route_df, stops_snapped = self._associate_blocks_with_routes(blocks)

        # Make a feature layer of blocks for further analysis
        with arcpy.EnvManager(overwriteOutput=True):
            full_block_obj = arcpy.management.MakeFeatureLayer(blocks, "Blocks layer").getOutput(0)

        # Identify blocks served by only a single block and by multiple blocks
        # Remove excess records so we have a single row per block-route pair
        block_route_df_dedup = block_route_df.drop_duplicates(subset=["block_id", "RouteName"])
        # Drop all records for blocks served by multiple routes to get the single-block routes
        single_route_blocks_df = block_route_df_dedup.drop_duplicates(subset=["block_id"], keep=False)
        single_route_blocks_df.set_index('block_id', inplace=True)
        # Do the opposite to keep records of blocks served by multiple routs
        multi_route_blocks_df = block_route_df_dedup[block_route_df_dedup.duplicated(subset=["block_id"], keep=False)]
        multi_route_blocks = list(multi_route_blocks_df["block_id"].unique())
        # Index full dataframe for lookups later
        block_route_df.set_index('block_id', inplace=True)

        # Block polygons will be assembled in separate feature classes and merged into one later
        block_polygon_fcs = []

        if len(single_route_blocks_df) > 0:
            self._write_progress_msg("Handling blocks served by a single route...")
            arcpy.management.AddField(blocks, "RouteName", "TEXT", field_length=500)
            where = f"{blocks_oid_field} in ({', '.join([str(b) for b in single_route_blocks_df.index.to_list()])})"
            with arcpy.da.UpdateCursor(   # pylint: disable=no-member
                blocks, [blocks_oid_field, 'RouteName'], where
            ) as uCur:
                for row in uCur:
                    row[1] = single_route_blocks_df.loc[row[0]]["RouteName"]
                    uCur.updateRow(row)
            arcpy.management.SelectLayerByAttribute(
                full_block_obj,
                "NEW_SELECTION",
                where
            )
            one_route_block_name = self._make_temporary_output_path("one_route_block")
            arcpy.management.CopyFeatures(full_block_obj, one_route_block_name)
            block_polygon_fcs.append(one_route_block_name)
            arcpy.management.DeleteField(blocks, 'RouteName')

        # Create Thiessen polygons for blocks with more than one route
        if len(multi_route_blocks) > 0:
            self._write_progress_msg("Handling blocks served by a multiple routes...")
            with arcpy.EnvManager(overwriteOutput=True):
                stops_snapped_lyr = arcpy.management.MakeFeatureLayer(stops_snapped, "SnappedStopsLyr")
            for i, block_oid in enumerate(multi_route_blocks):
                # Select the block
                arcpy.management.SelectLayerByAttribute(
                    full_block_obj,
                    "NEW_SELECTION",
                    f"{blocks_oid_field} = {block_oid}"
                )
                # Select the stops used by this block
                stops_in_block = [f"'{n}'" for n in list(block_route_df.loc[block_oid]["StopName"])]
                where = f"Name IN ({', '.join(stops_in_block)})"
                arcpy.management.SelectLayerByAttribute(stops_snapped_lyr, "NEW_SELECTION", where)

                # Create Thiessen polygons for the block using the stops in the block
                with arcpy.EnvManager(extent=blocks_extent, overwriteOutput=True):
                    # Note: Tool requires the Advanced license
                    thiessen_polys = self._make_temporary_output_path("thiessen_polys")
                    arcpy.analysis.CreateThiessenPolygons(stops_snapped_lyr, thiessen_polys, "ALL")
                    # Clip the Thiessen polygons to the boundaries of the block
                    temp_boundary_intersect_name = self._make_temporary_output_path("boundary_intersect" + str(i))
                    block_polygon_fcs.append(temp_boundary_intersect_name)
                    arcpy.analysis.PairwiseClip(
                        thiessen_polys,
                        full_block_obj,
                        temp_boundary_intersect_name
                    )

        # Merge all block polygons into a single feature class
        self._write_progress_msg("Creating final route geometries...")
        with arcpy.EnvManager(extent="MAXOF"):
            route_geometry_name = self._make_temporary_output_path("route_geometry")
            arcpy.management.Merge(block_polygon_fcs, route_geometry_name)

        # Dissolve polygon geometry by route name, yielding a single polygon for the area served by each route
        route_geometry_dissolve_name = self._make_temporary_output_path("route_geometry_dissolve")
        arcpy.analysis.PairwiseDissolve(
            in_features=route_geometry_name,
            out_feature_class=route_geometry_dissolve_name,
            dissolve_field="RouteName",
            multi_part="MULTI_PART"
        )

        # Populate the Routes sublayer with the new shapes
        self._update_route_geometry(route_geometry_dissolve_name)


class RouteGeometryGeneratorFromParcels(RouteGeometryGenerator):
    """Generate Waste Collection route geometries using parcel polygons associated with the Waste Collection Stops.

    Generate route geometries by associating parcels with stops and combining all parcels associated with a given route
    into a single polygon.  This method requires an external parcel dataset that is associated with the Stops sublayer
    of the Waste Collection layer by ID.
    """

    def __init__(self, waste_layer, parcel_data, parcel_id, stops_id):
        """Define the tool (tool name is the name of the class)."""
        super().__init__(waste_layer)
        self.parcel_data = parcel_data
        self.parcel_id = parcel_id
        self.stops_id = stops_id
        if int(arcpy.management.GetCount(self.parcel_data).getOutput(0)) == 0:
            raise RouteGeometryToolError("The parcel data is empty.")

    def create_route_geometry(self):
        """Create route geometry for the Waste Collection layer using parcels."""
        # Copy parcel data to avoid modifying the original data
        self._write_progress_msg("Copying parcels...")
        parcel_desc = arcpy.Describe(self.parcel_data)
        oid_field_name = parcel_desc.oidFieldName
        parcel_fields = [f.name for f in parcel_desc.fields]
        new_fields = [
            {
                "name": "RouteName",
                "aliasName": "RouteName",
                "type": "String",
                "length": 255
            }
        ]
        if self.parcel_id == oid_field_name:
            # Special handling for when the parcel ID field is the OID so we don't lose OIDs when copying
            out_field_name = "UniqueID"
            while out_field_name in parcel_fields:
                out_field_name += "1"
            new_fields.append({
                "name": out_field_name,
                "aliasName": out_field_name,
                "type": "BigInteger" if parcel_desc.hasOID64 else "Integer",
                "inputField": oid_field_name
            })
            self.parcel_id = out_field_name
        field_mappings = self._make_field_mappings(
            self.parcel_data,
            fields_to_delete=[f for f in parcel_fields if f != self.parcel_id],
            fields_to_add=new_fields
        )
        parcel_duplicated_name = self._make_temporary_output_path("parcels_duplicated")
        arcpy.conversion.ExportFeatures(
            in_features=self.parcel_data,
            out_features=parcel_duplicated_name,
            field_mapping=field_mappings
        )

        # Associate stops to parcels and parcels to routes
        self._write_progress_msg("Associating stops and parcels to routes...")
        stops_id_routename = {}
        all_stops_ids = set()
        all_parcel_ids = set()
        parcels_on_multiple_routes = set()
        for row in arcpy.da.SearchCursor(self.stops_sublayer, ['RouteName', self.stops_id]):  # pylint: disable=no-member
            route_name = row[0]
            stop_parcel_id = row[1]
            # Check for parcels assigned to multiple routes
            if stops_id_routename.get(stop_parcel_id, route_name) != route_name:
                parcels_on_multiple_routes.add(stop_parcel_id)
            stops_id_routename[stop_parcel_id] = route_name
            all_stops_ids.add(stop_parcel_id)
        with arcpy.da.UpdateCursor(  # pylint: disable=no-member
            parcel_duplicated_name, [self.parcel_id, "RouteName"]
        ) as uCur:
            for row in uCur:
                parcel_id_value = row[0]
                # Make sure the parcel ID values are actually unique
                if parcel_id_value in all_parcel_ids:
                    raise RouteGeometryToolError(
                        f"The parcel ID values in the {self.parcel_id} field in the parcel data are not unique.")
                all_parcel_ids.add(parcel_id_value)
                if parcel_id_value in stops_id_routename:
                    all_stops_ids.remove(parcel_id_value)
                    row[1] = stops_id_routename[parcel_id_value]
                    uCur.updateRow(row)
        if len(all_stops_ids) > 0:
            bad_stops_ids = list(all_stops_ids)[:10]
            arcpy.AddWarning((
                f"One or more stops have values in the {self.stops_id} field that do not correspond to parcel ID values "
                f"in the {self.parcel_id} field in the parcel data. The route polygons will not cover the parcels "
                f"associated with these stops. Bad {self.stops_id} field values (showing up to 10): {bad_stops_ids}"
            ))
        if len(parcels_on_multiple_routes) > 0:
            dup_stops_ids = list(parcels_on_multiple_routes)[:10]
            arcpy.AddWarning((
                f"One or more parcels have have multiple stops that are assigned to different routes. "
                f"The parcel's geometry will be assigned to only one route polygon. "
                f"Parcel {self.parcel_id} field values associated with multiple routes "
                f"(showing up to 10): {dup_stops_ids}"
            ))

        # Dissolve the parcels associated with each route into a single polygon
        self._write_progress_msg("Dissolving parcels into a single polygon associated with each route...")
        parcel_dissolved_name = self._make_temporary_output_path("parcels_dissolved")
        arcpy.analysis.PairwiseDissolve(
            in_features=parcel_duplicated_name,
            out_feature_class=parcel_dissolved_name,
            dissolve_field="RouteName",
            multi_part="MULTI_PART"
        )
        # If needed, project the dissolved parcel polygons to match the Waste Collection layer's spatial reference
        route_polygon_sr_obj = arcpy.Describe(self.routes_sublayer).spatialReference
        route_polygon_sr = route_polygon_sr_obj.name
        parcel_dissolved_sr = arcpy.Describe(parcel_dissolved_name).spatialReference.name
        if route_polygon_sr != parcel_dissolved_sr:
            self._write_progress_msg(
                "Projecting dissolved polygons to match Waste Collection layer's spatial reference...")
            parcel_dissolved_projected_name = self._make_temporary_output_path("parcels_dissolve_projected")
            arcpy.management.Project(
                in_dataset=parcel_dissolved_name,
                out_dataset=parcel_dissolved_projected_name,
                out_coor_system=route_polygon_sr_obj
            )
            parcel_dissolved_name = parcel_dissolved_projected_name

        # Update the route polygon shapes
        self._update_route_geometry(parcel_dissolved_name)


class RouteGeometryToolError(Exception):
    """Generic error class that can be raised for known problems in these tools to stop tool execution."""

    def __init__(self, msg):  # pylint:disable=super-init-not-called
        """Raise an error."""
        arcpy.AddError(msg)
