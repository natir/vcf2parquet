var searchIndex = JSON.parse('{\
"vcf2parquet":{"doc":"vcf2parquet allow user to convert a vcf in parquet format.","t":[0,0,0,0,5,5,13,4,13,13,13,13,6,11,11,11,11,11,11,11,11,11,11,5,11,11,11,11,11,11,12,12,12,12,13,4,13,13,13,13,13,13,3,13,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,12,12,12,12,12,12,12,3,11,11,11,11,11,11,11,11,11,11,11,5],"n":["error","name2data","record2chunk","schema","vcf2multiparquet","vcf2parquet","Arrow","Error","Io","NoConversion","NoodlesHeader","Parquet","Result","borrow","borrow_mut","fmt","fmt","from","from","from","from","from","into","mapping","provide","source","to_string","try_from","try_into","type_id","error","error","error","error","Bool","ColumnData","Float","Int","ListBool","ListFloat","ListInt","ListString","Name2Data","String","add_record","borrow","borrow","borrow_mut","borrow_mut","fmt","fmt","from","from","get","get_mut","into","into","into_arc","into_arc","new","push_bool","push_f32","push_i32","push_null","push_string","push_vecbool","push_vecf32","push_veci32","push_vecstring","try_from","try_from","try_into","try_into","type_id","type_id","0","0","0","0","0","0","0","0","Record2Chunk","borrow","borrow_mut","encodings","from","into","into_iter","new","next","try_from","try_into","type_id","from_header"],"q":["vcf2parquet","","","","","","vcf2parquet::error","","","","","","","","","","","","","","","","","","","","","","","","vcf2parquet::error::Error","","","","vcf2parquet::name2data","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","vcf2parquet::name2data::ColumnData","","","","","","","","vcf2parquet::record2chunk","","","","","","","","","","","","vcf2parquet::schema"],"d":["vcf2parquet error","Struct to link name and data","Convert vcf record iterator into Parquet chunk","Construct parquet schema corresponding to vcf","Read <code>input</code> vcf and write each row group in a parquet file …","Read <code>input</code> vcf and write parquet in <code>output</code>","Arrow error","","Io error","Not support type conversion","Noodles header vcf error","Parquet error","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","","","","","","","","","","","","","","Alias of std::collections::HashMap that associate a column …","","Add a vcf record in std::collections::HashMap struct","","","","","","","Returns the argument unchanged.","Returns the argument unchanged.","Just a wrapper arround std::collections::HashMap::get","Just a wrapper arround std::collections::HashMap::get_mut","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Convert Name2Data in vector of arrow2 array","Convert ColumnData in Arrow2 array","Create a new Name2Data, vcf header is required to add info …","Add a boolean value in array, if it’s not a boolean …","Add a f32 value in array, if it’s not a float array …","Add a i32 value in array, if it’s not a integer array …","Add a Null value in array","Add a string value in array, if it’s not a string array …","Add a vector of bool value in array, if it’s not a …","Add a vector of float value in array, if it’s not a …","Add a vector of integer value in array, if it’s not a …","Add a vector of string value in array, if it’s not a …","","","","","","","","","","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","Generate a parquet schema corresponding to vcf header"],"i":[0,0,0,0,0,0,4,0,4,4,4,4,0,4,4,4,4,4,4,4,4,4,4,0,4,4,4,4,4,4,32,33,34,35,20,0,20,20,20,20,20,20,0,20,17,17,20,17,20,17,20,17,20,17,17,17,20,17,20,17,20,20,20,20,20,20,20,20,20,17,20,17,20,17,20,36,37,38,39,40,41,42,43,0,29,29,29,29,29,29,29,29,29,29,29,0],"f":[0,0,0,0,[[1,2,3],[[5,[4]]]],[[2,3],[[5,[4]]]],0,0,0,0,0,0,0,[[]],[[]],[[4,6],[[5,[7]]]],[[4,6],[[5,[7]]]],[8,4],[9,4],[10,4],[11,4],[[]],[[]],[[],4],[12],[4,[[14,[13]]]],[[],15],[[],5],[[],5],[[],16],0,0,0,0,0,0,0,0,0,0,0,0,0,0,[[17,18,19],[[5,[8]]]],[[]],[[]],[[]],[[]],[[17,6],[[5,[7]]]],[[20,6],[[5,[7]]]],[[]],[[]],[[17,1],[[14,[20]]]],[[17,1],[[14,[20]]]],[[]],[[]],[[17,21],[[25,[[23,[22]],24]]]],[20,[[23,[22]]]],[[2,19],17],[[20,[14,[26]]]],[[20,[14,[27]]]],[[20,[14,[28]]]],[20],[[20,15]],[[20,[25,[[14,[26]],24]]],[[5,[8]]]],[[20,[25,[[14,[27]],24]]],[[5,[8]]]],[[20,[25,[[14,[28]],24]]],[[5,[8]]]],[[20,[25,[[14,[15]],24]]],[[5,[8]]]],[[],5],[[],5],[[],5],[[],5],[[],16],[[],16],0,0,0,0,0,0,0,0,0,[[]],[[]],[29,[[25,[[25,[30,24]],24]]]],[[]],[[]],[[]],[[31,2,19,21],29],[29,14],[[],5],[[],5],[[],16],[19,[[5,[21,4]]]]],"p":[[15,"str"],[15,"usize"],[4,"CompressionOptions"],[4,"Error"],[4,"Result"],[3,"Formatter"],[3,"Error"],[4,"Error"],[4,"ParseError"],[4,"Error"],[3,"Error"],[3,"Demand"],[8,"Error"],[4,"Option"],[3,"String"],[3,"TypeId"],[3,"Name2Data"],[3,"Record"],[3,"Header"],[4,"ColumnData"],[3,"Schema"],[8,"Array"],[3,"Arc"],[3,"Global"],[3,"Vec"],[15,"bool"],[15,"f32"],[15,"i32"],[3,"Record2Chunk"],[4,"Encoding"],[3,"Records"],[13,"Arrow"],[13,"Parquet"],[13,"Io"],[13,"NoodlesHeader"],[13,"Bool"],[13,"Int"],[13,"Float"],[13,"String"],[13,"ListBool"],[13,"ListInt"],[13,"ListFloat"],[13,"ListString"]]},\
"vcf2parquet_bin":{"doc":"vcf2parquet bin","t":[0,0,5,13,3,4,3,13,13,13,13,13,3,13,4,13,13,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,12,4,13,13,13,6,11,11,11,11,11,11,11,11,11,5,11,11,11,11,11,11,12,12,12],"n":["cli","error","main","Brotli","Command","Compression","Convert","Convert","Gzip","Lz4","Lzo","Snappy","Split","Split","SubCommand","Uncompressed","Zstd","__clone_box","__clone_box","__clone_box","__clone_box","augment_args","augment_args","augment_args","augment_args_for_update","augment_args_for_update","augment_args_for_update","augment_subcommands","augment_subcommands_for_update","batch_size","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","clone","clone","clone","clone","clone_into","clone_into","clone_into","clone_into","command","command","command","command","command_for_update","command_for_update","command_for_update","command_for_update","compression","fmt","fmt","fmt","fmt","fmt","format","from","from","from","from","from","from_arg_matches","from_arg_matches","from_arg_matches","from_arg_matches","from_arg_matches_mut","from_arg_matches_mut","from_arg_matches_mut","from_arg_matches_mut","group_id","group_id","group_id","has_subcommand","input","into","into","into","into","into","output","read_buffer","subcommand","to_owned","to_owned","to_owned","to_owned","to_possible_value","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","update_from_arg_matches","update_from_arg_matches","update_from_arg_matches","update_from_arg_matches","update_from_arg_matches_mut","update_from_arg_matches_mut","update_from_arg_matches_mut","update_from_arg_matches_mut","value_variants","0","0","Error","Io","Lib","Niffler","Result","borrow","borrow_mut","fmt","fmt","from","from","from","from","into","mapping","provide","source","to_string","try_from","try_into","type_id","error","error","error"],"q":["vcf2parquet_bin","","","vcf2parquet_bin::cli","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","vcf2parquet_bin::cli::SubCommand","","vcf2parquet_bin::error","","","","","","","","","","","","","","","","","","","","","vcf2parquet_bin::error::Error","",""],"d":["cli of vcf2parquet-bin","error of vcf2parquet-bin","","","","","Convert a vcf in a parquet","","","","","","Convert a vcf in multiple parquet file each file contains …","","","","","","","","","","","","","","","","","Get batch_size set by user or default value","","","","","","","","","","","","","","","","","","","","","","","","","","","Get compression set by user or default value","","","","","","Get output format","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","","","","","","","","","","","","","Get input","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Get output","Get read buffer size","Get subcommand","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","Io error","vcf2parquet-lib error","Niffler error","","","","","","Returns the argument unchanged.","","","","Calls <code>U::from(self)</code>.","","","","","","","","","",""],"i":[0,0,0,6,0,0,0,7,6,6,6,6,0,7,0,6,6,6,7,8,9,4,8,9,4,8,9,7,7,4,6,4,7,8,9,6,4,7,8,9,6,7,8,9,6,7,8,9,4,7,8,9,4,7,8,9,4,6,4,7,8,9,9,6,4,7,8,9,4,7,8,9,4,7,8,9,4,8,9,7,4,6,4,7,8,9,8,4,4,6,7,8,9,6,6,4,7,8,9,6,4,7,8,9,6,4,7,8,9,4,7,8,9,4,7,8,9,6,30,31,0,23,23,23,0,23,23,23,23,23,23,23,23,23,0,23,23,23,23,23,23,32,33,34],"f":[0,0,[[],1],0,0,0,0,0,0,0,0,0,0,0,0,0,0,[2],[2],[2],[2],[3,3],[3,3],[3,3],[3,3],[3,3],[3,3],[3,3],[3,3],[4,5],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[[]],[6,6],[7,7],[8,8],[9,9],[[]],[[]],[[]],[[]],[[],3],[[],3],[[],3],[[],3],[[],3],[[],3],[[],3],[[],3],[4,10],[[6,11],12],[[4,11],12],[[7,11],12],[[8,11],12],[[9,11],12],[9,13],[[]],[[]],[[]],[[]],[[]],[14,[[16,[4,15]]]],[14,[[16,[7,15]]]],[14,[[16,[8,15]]]],[14,[[16,[9,15]]]],[14,[[16,[4,15]]]],[14,[[16,[7,15]]]],[14,[[16,[8,15]]]],[14,[[16,[9,15]]]],[[],[[18,[17]]]],[[],[[18,[17]]]],[[],[[18,[17]]]],[13,19],[4,20],[[]],[[]],[[]],[[]],[[]],[8,20],[4,5],[4,7],[[]],[[]],[[]],[[]],[6,[[18,[21]]]],[[],16],[[],16],[[],16],[[],16],[[],16],[[],16],[[],16],[[],16],[[],16],[[],16],[[],22],[[],22],[[],22],[[],22],[[],22],[[4,14],[[16,[15]]]],[[7,14],[[16,[15]]]],[[8,14],[[16,[15]]]],[[9,14],[[16,[15]]]],[[4,14],[[16,[15]]]],[[7,14],[[16,[15]]]],[[8,14],[[16,[15]]]],[[9,14],[[16,[15]]]],[[]],0,0,0,0,0,0,0,[[]],[[]],[[23,11],12],[[23,11],12],[[]],[24,23],[25,23],[26,23],[[]],[[],23],[27],[23,[[18,[28]]]],[[],29],[[],16],[[],16],[[],22],0,0,0],"p":[[6,"Result"],[3,"Private"],[3,"Command"],[3,"Command"],[15,"usize"],[4,"Compression"],[4,"SubCommand"],[3,"Convert"],[3,"Split"],[4,"CompressionOptions"],[3,"Formatter"],[6,"Result"],[15,"str"],[3,"ArgMatches"],[6,"Error"],[4,"Result"],[3,"Id"],[4,"Option"],[15,"bool"],[3,"PathBuf"],[3,"PossibleValue"],[3,"TypeId"],[4,"Error"],[3,"Error"],[4,"Error"],[4,"Error"],[3,"Demand"],[8,"Error"],[3,"String"],[13,"Convert"],[13,"Split"],[13,"Io"],[13,"Niffler"],[13,"Lib"]]},\
"vcf2parquet_lib":{"doc":"vcf2parquet library","t":[0,0,0,0,5,5,13,4,13,13,13,13,6,11,11,11,11,11,11,11,11,11,11,5,11,11,11,11,11,11,12,12,12,12,13,4,13,13,13,13,13,13,3,13,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,12,12,12,12,12,12,12,12,3,11,11,11,11,11,11,11,11,11,11,11,5],"n":["error","name2data","record2chunk","schema","vcf2multiparquet","vcf2parquet","Arrow","Error","Io","NoConversion","NoodlesHeader","Parquet","Result","borrow","borrow_mut","fmt","fmt","from","from","from","from","from","into","mapping","provide","source","to_string","try_from","try_into","type_id","error","error","error","error","Bool","ColumnData","Float","Int","ListBool","ListFloat","ListInt","ListString","Name2Data","String","add_record","borrow","borrow","borrow_mut","borrow_mut","fmt","fmt","from","from","get","get_mut","into","into","into_arc","into_arc","new","push_bool","push_f32","push_i32","push_null","push_string","push_vecbool","push_vecf32","push_veci32","push_vecstring","try_from","try_from","try_into","try_into","type_id","type_id","0","0","0","0","0","0","0","0","Record2Chunk","borrow","borrow_mut","encodings","from","into","into_iter","new","next","try_from","try_into","type_id","from_header"],"q":["vcf2parquet_lib","","","","","","vcf2parquet_lib::error","","","","","","","","","","","","","","","","","","","","","","","","vcf2parquet_lib::error::Error","","","","vcf2parquet_lib::name2data","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","vcf2parquet_lib::name2data::ColumnData","","","","","","","","vcf2parquet_lib::record2chunk","","","","","","","","","","","","vcf2parquet_lib::schema"],"d":["vcf2parquet error","Struct to link name and data","Convert vcf record iterator into Parquet chunk","Construct parquet schema corresponding to vcf","Read <code>input</code> vcf and write each row group in a parquet file …","Read <code>input</code> vcf and write parquet in <code>output</code>","Arrow error","","Io error","Not support type conversion","Noodles header vcf error","Parquet error","","","","","","","","Returns the argument unchanged.","","","Calls <code>U::from(self)</code>.","","","","","","","","","","","","","","","","","","","","Alias of std::collections::HashMap that associate a column …","","Add a vcf record in std::collections::HashMap struct","","","","","","","Returns the argument unchanged.","Returns the argument unchanged.","Just a wrapper arround std::collections::HashMap::get","Just a wrapper arround std::collections::HashMap::get_mut","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Convert Name2Data in vector of arrow2 array","Convert ColumnData in Arrow2 array","Create a new Name2Data, vcf header is required to add info …","Add a boolean value in array, if it’s not a boolean …","Add a f32 value in array, if it’s not a float array …","Add a i32 value in array, if it’s not a integer array …","Add a Null value in array","Add a string value in array, if it’s not a string array …","Add a vector of bool value in array, if it’s not a …","Add a vector of float value in array, if it’s not a …","Add a vector of integer value in array, if it’s not a …","Add a vector of string value in array, if it’s not a …","","","","","","","","","","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","Generate a parquet schema corresponding to vcf header"],"i":[0,0,0,0,0,0,5,0,5,5,5,5,0,5,5,5,5,5,5,5,5,5,5,0,5,5,5,5,5,5,33,34,35,36,21,0,21,21,21,21,21,21,0,21,18,18,21,18,21,18,21,18,21,18,18,18,21,18,21,18,21,21,21,21,21,21,21,21,21,18,21,18,21,18,21,37,38,39,40,41,42,43,44,0,30,30,30,30,30,30,30,30,30,30,30,0],"f":[0,0,0,0,[[1,2,3],4],[[2,3],4],0,0,0,0,0,0,0,[[]],[[]],[[5,6],7],[[5,6],7],[8,5],[9,5],[[]],[10,5],[11,5],[[]],[[],5],[12],[5,[[14,[13]]]],[[],15],[[],16],[[],16],[[],17],0,0,0,0,0,0,0,0,0,0,0,0,0,0,[[18,19,20],[[16,[9]]]],[[]],[[]],[[]],[[]],[[18,6],7],[[21,6],7],[[]],[[]],[[18,1],[[14,[21]]]],[[18,1],[[14,[21]]]],[[]],[[]],[[18,22],[[25,[[24,[23]]]]]],[21,[[24,[23]]]],[[2,20],18],[[21,[14,[26]]]],[[21,[14,[27]]]],[[21,[14,[28]]]],[21],[[21,15]],[[21,[25,[[14,[26]]]]],29],[[21,[25,[[14,[27]]]]],29],[[21,[25,[[14,[28]]]]],29],[[21,[25,[[14,[15]]]]],29],[[],16],[[],16],[[],16],[[],16],[[],17],[[],17],0,0,0,0,0,0,0,0,0,[[]],[[]],[30,[[25,[[25,[31]]]]]],[[]],[[]],[[]],[[32,2,20,22],30],[30,14],[[],16],[[],16],[[],17],[20,[[4,[22]]]]],"p":[[15,"str"],[15,"usize"],[4,"CompressionOptions"],[6,"Result"],[4,"Error"],[3,"Formatter"],[6,"Result"],[4,"Error"],[4,"Error"],[3,"Error"],[4,"ParseError"],[3,"Demand"],[8,"Error"],[4,"Option"],[3,"String"],[4,"Result"],[3,"TypeId"],[3,"Name2Data"],[3,"Record"],[3,"Header"],[4,"ColumnData"],[3,"Schema"],[8,"Array"],[3,"Arc"],[3,"Vec"],[15,"bool"],[15,"f32"],[15,"i32"],[6,"Result"],[3,"Record2Chunk"],[4,"Encoding"],[3,"Records"],[13,"Arrow"],[13,"Parquet"],[13,"Io"],[13,"NoodlesHeader"],[13,"Bool"],[13,"Int"],[13,"Float"],[13,"String"],[13,"ListBool"],[13,"ListInt"],[13,"ListFloat"],[13,"ListString"]]}\
}');
if (typeof window !== 'undefined' && window.initSearch) {window.initSearch(searchIndex)};
if (typeof exports !== 'undefined') {exports.searchIndex = searchIndex};
