#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: scalapb/scalapb.proto
"""Generated protocol buffer code."""
from google.protobuf import (
    descriptor as _descriptor,
    descriptor_pool as _descriptor_pool,
    symbol_database as _symbol_database,
)
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import descriptor_pb2 as google_dot_protobuf_dot_descriptor__pb2

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x15scalapb/scalapb.proto\x12\x07scalapb\x1a google/protobuf/descriptor.proto"\xec\x0b\n\x0eScalaPbOptions\x12\x14\n\x0cpackage_name\x18\x01 \x01(\t\x12\x14\n\x0c\x66lat_package\x18\x02 \x01(\x08\x12\x0e\n\x06import\x18\x03 \x03(\t\x12\x10\n\x08preamble\x18\x04 \x03(\t\x12\x13\n\x0bsingle_file\x18\x05 \x01(\x08\x12\x1d\n\x15no_primitive_wrappers\x18\x07 \x01(\x08\x12\x1a\n\x12primitive_wrappers\x18\x06 \x01(\x08\x12\x17\n\x0f\x63ollection_type\x18\x08 \x01(\t\x12%\n\x17preserve_unknown_fields\x18\t \x01(\x08:\x04true\x12\x13\n\x0bobject_name\x18\n \x01(\t\x12\x33\n\x05scope\x18\x0b \x01(\x0e\x32$.scalapb.ScalaPbOptions.OptionsScope\x12\x14\n\x06lenses\x18\x0c \x01(\x08:\x04true\x12\x1f\n\x17retain_source_code_info\x18\r \x01(\x08\x12\x10\n\x08map_type\x18\x0e \x01(\t\x12(\n no_default_values_in_constructor\x18\x0f \x01(\x08\x12\x42\n\x11\x65num_value_naming\x18\x10 \x01(\x0e\x32\'.scalapb.ScalaPbOptions.EnumValueNaming\x12 \n\x11\x65num_strip_prefix\x18\x11 \x01(\x08:\x05\x66\x61lse\x12\x12\n\nbytes_type\x18\x15 \x01(\t\x12\x18\n\x10java_conversions\x18\x17 \x01(\x08\x12\x46\n\x13\x61ux_message_options\x18\x12 \x03(\x0b\x32).scalapb.ScalaPbOptions.AuxMessageOptions\x12\x42\n\x11\x61ux_field_options\x18\x13 \x03(\x0b\x32\'.scalapb.ScalaPbOptions.AuxFieldOptions\x12@\n\x10\x61ux_enum_options\x18\x14 \x03(\x0b\x32&.scalapb.ScalaPbOptions.AuxEnumOptions\x12K\n\x16\x61ux_enum_value_options\x18\x16 \x03(\x0b\x32+.scalapb.ScalaPbOptions.AuxEnumValueOptions\x12\x15\n\rpreprocessors\x18\x18 \x03(\t\x12;\n\x15\x66ield_transformations\x18\x19 \x03(\x0b\x32\x1c.scalapb.FieldTransformation\x12"\n\x1aignore_all_transformations\x18\x1a \x01(\x08\x12\x15\n\x07getters\x18\x1b \x01(\x08:\x04true\x12\x16\n\x0escala3_sources\x18\x1c \x01(\x08\x12%\n\x1dpublic_constructor_parameters\x18\x1d \x01(\x08\x12&\n\x1dtest_only_no_java_conversions\x18\xe7\x07 \x01(\x08\x1aM\n\x11\x41uxMessageOptions\x12\x0e\n\x06target\x18\x01 \x01(\t\x12(\n\x07options\x18\x02 \x01(\x0b\x32\x17.scalapb.MessageOptions\x1aI\n\x0f\x41uxFieldOptions\x12\x0e\n\x06target\x18\x01 \x01(\t\x12&\n\x07options\x18\x02 \x01(\x0b\x32\x15.scalapb.FieldOptions\x1aG\n\x0e\x41uxEnumOptions\x12\x0e\n\x06target\x18\x01 \x01(\t\x12%\n\x07options\x18\x02 \x01(\x0b\x32\x14.scalapb.EnumOptions\x1aQ\n\x13\x41uxEnumValueOptions\x12\x0e\n\x06target\x18\x01 \x01(\t\x12*\n\x07options\x18\x02 \x01(\x0b\x32\x19.scalapb.EnumValueOptions"%\n\x0cOptionsScope\x12\x08\n\x04\x46ILE\x10\x00\x12\x0b\n\x07PACKAGE\x10\x01"2\n\x0f\x45numValueNaming\x12\x0f\n\x0b\x41S_IN_PROTO\x10\x00\x12\x0e\n\nCAMEL_CASE\x10\x01*\t\x08\xe8\x07\x10\x80\x80\x80\x80\x02"\x80\x03\n\x0eMessageOptions\x12\x0f\n\x07\x65xtends\x18\x01 \x03(\t\x12\x19\n\x11\x63ompanion_extends\x18\x02 \x03(\t\x12\x13\n\x0b\x61nnotations\x18\x03 \x03(\t\x12\x0c\n\x04type\x18\x04 \x01(\t\x12\x1d\n\x15\x63ompanion_annotations\x18\x05 \x03(\t\x12\x1c\n\x14sealed_oneof_extends\x18\x06 \x03(\t\x12\x0e\n\x06no_box\x18\x07 \x01(\x08\x12"\n\x1aunknown_fields_annotations\x18\x08 \x03(\t\x12(\n no_default_values_in_constructor\x18\t \x01(\x08\x12&\n\x1esealed_oneof_companion_extends\x18\n \x03(\t\x12\x0f\n\x07\x64\x65rives\x18\x0b \x03(\t\x12\x1c\n\x14sealed_oneof_derives\x18\x0c \x03(\t\x12"\n\x1asealed_oneof_empty_extends\x18\r \x03(\t*\t\x08\xe8\x07\x10\x80\x80\x80\x80\x02">\n\nCollection\x12\x0c\n\x04type\x18\x01 \x01(\t\x12\x11\n\tnon_empty\x18\x02 \x01(\x08\x12\x0f\n\x07\x61\x64\x61pter\x18\x03 \x01(\t"\x95\x02\n\x0c\x46ieldOptions\x12\x0c\n\x04type\x18\x01 \x01(\t\x12\x12\n\nscala_name\x18\x02 \x01(\t\x12\x17\n\x0f\x63ollection_type\x18\x03 \x01(\t\x12\'\n\ncollection\x18\x08 \x01(\x0b\x32\x13.scalapb.Collection\x12\x10\n\x08key_type\x18\x04 \x01(\t\x12\x12\n\nvalue_type\x18\x05 \x01(\t\x12\x13\n\x0b\x61nnotations\x18\x06 \x03(\t\x12\x10\n\x08map_type\x18\x07 \x01(\t\x12\'\n\x1fno_default_value_in_constructor\x18\t \x01(\x08\x12\x0e\n\x06no_box\x18\x1e \x01(\x08\x12\x10\n\x08required\x18\x1f \x01(\x08*\t\x08\xe8\x07\x10\x80\x80\x80\x80\x02"\xae\x01\n\x0b\x45numOptions\x12\x0f\n\x07\x65xtends\x18\x01 \x03(\t\x12\x19\n\x11\x63ompanion_extends\x18\x02 \x03(\t\x12\x0c\n\x04type\x18\x03 \x01(\t\x12\x18\n\x10\x62\x61se_annotations\x18\x04 \x03(\t\x12\x1e\n\x16recognized_annotations\x18\x05 \x03(\t\x12 \n\x18unrecognized_annotations\x18\x06 \x03(\t*\t\x08\xe8\x07\x10\x80\x80\x80\x80\x02"W\n\x10\x45numValueOptions\x12\x0f\n\x07\x65xtends\x18\x01 \x03(\t\x12\x12\n\nscala_name\x18\x02 \x01(\t\x12\x13\n\x0b\x61nnotations\x18\x03 \x03(\t*\t\x08\xe8\x07\x10\x80\x80\x80\x80\x02">\n\x0cOneofOptions\x12\x0f\n\x07\x65xtends\x18\x01 \x03(\t\x12\x12\n\nscala_name\x18\x02 \x01(\t*\t\x08\xe8\x07\x10\x80\x80\x80\x80\x02"\xa8\x01\n\x13\x46ieldTransformation\x12\x33\n\x04when\x18\x01 \x01(\x0b\x32%.google.protobuf.FieldDescriptorProto\x12\x30\n\nmatch_type\x18\x02 \x01(\x0e\x32\x12.scalapb.MatchType:\x08\x43ONTAINS\x12*\n\x03set\x18\x03 \x01(\x0b\x32\x1d.google.protobuf.FieldOptions"\xac\x01\n\x12PreprocessorOutput\x12G\n\x0foptions_by_file\x18\x01 \x03(\x0b\x32..scalapb.PreprocessorOutput.OptionsByFileEntry\x1aM\n\x12OptionsByFileEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12&\n\x05value\x18\x02 \x01(\x0b\x32\x17.scalapb.ScalaPbOptions:\x02\x38\x01*2\n\tMatchType\x12\x0c\n\x08\x43ONTAINS\x10\x00\x12\t\n\x05\x45XACT\x10\x01\x12\x0c\n\x08PRESENCE\x10\x02:G\n\x07options\x12\x1c.google.protobuf.FileOptions\x18\xfc\x07 \x01(\x0b\x32\x17.scalapb.ScalaPbOptions:J\n\x07message\x12\x1f.google.protobuf.MessageOptions\x18\xfc\x07 \x01(\x0b\x32\x17.scalapb.MessageOptions:D\n\x05\x66ield\x12\x1d.google.protobuf.FieldOptions\x18\xfc\x07 \x01(\x0b\x32\x15.scalapb.FieldOptions:I\n\x0c\x65num_options\x12\x1c.google.protobuf.EnumOptions\x18\xfc\x07 \x01(\x0b\x32\x14.scalapb.EnumOptions:Q\n\nenum_value\x12!.google.protobuf.EnumValueOptions\x18\xfc\x07 \x01(\x0b\x32\x19.scalapb.EnumValueOptions:D\n\x05oneof\x12\x1d.google.protobuf.OneofOptions\x18\xfc\x07 \x01(\x0b\x32\x15.scalapb.OneofOptionsBK\n\x0fscalapb.optionsZ"scalapb.github.io/protobuf/scalapb\xe2?\x13\n\x0fscalapb.options\x10\x01'
)

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "scalapb.scalapb_pb2", globals())
if _descriptor._USE_C_DESCRIPTORS == False:
    google_dot_protobuf_dot_descriptor__pb2.FileOptions.RegisterExtension(options)
    google_dot_protobuf_dot_descriptor__pb2.MessageOptions.RegisterExtension(message)
    google_dot_protobuf_dot_descriptor__pb2.FieldOptions.RegisterExtension(field)
    google_dot_protobuf_dot_descriptor__pb2.EnumOptions.RegisterExtension(enum_options)
    google_dot_protobuf_dot_descriptor__pb2.EnumValueOptions.RegisterExtension(
        enum_value
    )
    google_dot_protobuf_dot_descriptor__pb2.OneofOptions.RegisterExtension(oneof)

    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b'\n\017scalapb.optionsZ"scalapb.github.io/protobuf/scalapb\342?\023\n\017scalapb.options\020\001'
    _PREPROCESSOROUTPUT_OPTIONSBYFILEENTRY._options = None
    _PREPROCESSOROUTPUT_OPTIONSBYFILEENTRY._serialized_options = b"8\001"
    _MATCHTYPE._serialized_start = 2994
    _MATCHTYPE._serialized_end = 3044
    _SCALAPBOPTIONS._serialized_start = 69
    _SCALAPBOPTIONS._serialized_end = 1585
    _SCALAPBOPTIONS_AUXMESSAGEOPTIONS._serialized_start = 1175
    _SCALAPBOPTIONS_AUXMESSAGEOPTIONS._serialized_end = 1252
    _SCALAPBOPTIONS_AUXFIELDOPTIONS._serialized_start = 1254
    _SCALAPBOPTIONS_AUXFIELDOPTIONS._serialized_end = 1327
    _SCALAPBOPTIONS_AUXENUMOPTIONS._serialized_start = 1329
    _SCALAPBOPTIONS_AUXENUMOPTIONS._serialized_end = 1400
    _SCALAPBOPTIONS_AUXENUMVALUEOPTIONS._serialized_start = 1402
    _SCALAPBOPTIONS_AUXENUMVALUEOPTIONS._serialized_end = 1483
    _SCALAPBOPTIONS_OPTIONSSCOPE._serialized_start = 1485
    _SCALAPBOPTIONS_OPTIONSSCOPE._serialized_end = 1522
    _SCALAPBOPTIONS_ENUMVALUENAMING._serialized_start = 1524
    _SCALAPBOPTIONS_ENUMVALUENAMING._serialized_end = 1574
    _MESSAGEOPTIONS._serialized_start = 1588
    _MESSAGEOPTIONS._serialized_end = 1972
    _COLLECTION._serialized_start = 1974
    _COLLECTION._serialized_end = 2036
    _FIELDOPTIONS._serialized_start = 2039
    _FIELDOPTIONS._serialized_end = 2316
    _ENUMOPTIONS._serialized_start = 2319
    _ENUMOPTIONS._serialized_end = 2493
    _ENUMVALUEOPTIONS._serialized_start = 2495
    _ENUMVALUEOPTIONS._serialized_end = 2582
    _ONEOFOPTIONS._serialized_start = 2584
    _ONEOFOPTIONS._serialized_end = 2646
    _FIELDTRANSFORMATION._serialized_start = 2649
    _FIELDTRANSFORMATION._serialized_end = 2817
    _PREPROCESSOROUTPUT._serialized_start = 2820
    _PREPROCESSOROUTPUT._serialized_end = 2992
    _PREPROCESSOROUTPUT_OPTIONSBYFILEENTRY._serialized_start = 2915
    _PREPROCESSOROUTPUT_OPTIONSBYFILEENTRY._serialized_end = 2992
# @@protoc_insertion_point(module_scope)
