var searchIndex = new Map(JSON.parse('[\
["pyvcf2parquet",{"doc":"","t":"PGPPPPPPNNNNNNNNNNNNNNNNNNNNN","n":["Brotli","Compression","Gzip","Lz4","Lzo","Snappy","Uncompressed","Zstd","__clone_box","arguments","borrow","borrow_mut","clone","clone_into","doc","extract","extract","extract","fmt","from","into","into_py","items_iter","lazy_type_object","to_owned","try_from","try_into","type_id","type_object_raw"],"q":[[0,"pyvcf2parquet"],[29,"dyn_clone::sealed"],[30,"pyo3::marker"],[31,"pyo3::types::any"],[32,"pyo3::instance"],[33,"core::ffi::c_str"],[34,"pyo3::err"],[35,"pyo3::err"],[36,"core::fmt"],[37,"core::fmt"],[38,"pyo3::impl_::pyclass::lazy_type_object"],[39,"core::any"],[40,"pyo3_ffi::cpython::object"]],"d":["","","","","","","","","","","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","",""],"i":[6,0,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6],"f":[0,0,0,0,0,0,0,0,[[-1,1],2,[]],[[-1,3],[[5,[4]]],[]],[-1,-2,[],[]],[-1,-2,[],[]],[6,6],[[-1,-2],2,[],[]],[3,[[8,[7]]]],[4,[[10,[-1,9]]],[]],[[4,-1],[[8,[6]]],[]],[[4,-1],[[8,[6]]],[]],[[6,11],12],[-1,-1,[]],[-1,-2,[],[]],[[6,3],13],[[],14],[[],[[15,[6]]]],[-1,-2,[],[]],[-1,[[10,[-2]]],[],[]],[-1,[[10,[-2]]],[],[]],[-1,16,[]],[3,17]],"c":[],"p":[[5,"Private",29],[1,"tuple"],[5,"Python",30],[5,"PyAny",31],[5,"Py",32],[6,"Compression",0],[5,"CStr",33],[8,"PyResult",34],[5,"PyErr",34],[6,"Result",35],[5,"Formatter",36],[8,"Result",36],[8,"PyObject",32],[5,"PyClassItemsIter",37],[5,"LazyTypeObject",38],[5,"TypeId",39],[5,"PyTypeObject",40]],"b":[[16,"impl-PyFunctionArgument%3C\'a,+\'py%3E-for-%26Compression"],[17,"impl-PyFunctionArgument%3C\'a,+\'py%3E-for-%26mut+Compression"]]}],\
["vcf2parquet",{"doc":"vcf2parquet allow user to convert a vcf in parquet format.","t":"CCCCHHPPGPPPPPINNNNNNNNNNNNNNNPGPPPPPPFPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNFNNNNNNNNNNNH","n":["error","name2data","record2chunk","schema","vcf2multiparquet","vcf2parquet","Arrow","Err","Error","Io","NoConversion","NoodlesHeader","Ok","Parquet","Result","borrow","borrow_mut","fmt","fmt","from","from","from","from","from","into","source","to_string","try_from","try_into","type_id","Bool","ColumnData","Float","Int","ListBool","ListFloat","ListInt","ListString","Name2Data","String","add_record","borrow","borrow","borrow_mut","borrow_mut","fmt","fmt","from","from","get","get_mut","into","into","into_arc","into_arc","new","push_bool","push_f32","push_i32","push_null","push_string","push_vecbool","push_vecf32","push_veci32","push_vecstring","try_from","try_from","try_into","try_into","type_id","type_id","Record2Chunk","borrow","borrow_mut","encodings","from","into","into_iter","new","next","try_from","try_into","type_id","from_header"],"q":[[0,"vcf2parquet"],[6,"vcf2parquet::error"],[30,"vcf2parquet::name2data"],[71,"vcf2parquet::record2chunk"],[83,"vcf2parquet::schema"],[84,"parquet2::parquet_bridge"],[85,"core::result"],[86,"std::io"],[87,"std::io"],[88,"core::fmt"],[89,"parquet2::error"],[90,"noodles_vcf::header::parser"],[91,"std::io::error"],[92,"core::error"],[93,"core::option"],[94,"alloc::string"],[95,"core::any"],[96,"noodles_vcf::record"],[97,"noodles_vcf::header"],[98,"arrow2::datatypes::schema"],[99,"arrow2::array"],[100,"alloc::sync"],[101,"alloc::vec"],[102,"parquet2::parquet_bridge"]],"d":["vcf2parquet error","Struct to link name and data","Convert vcf record iterator into Parquet chunk","Construct parquet schema corresponding to vcf","Read <code>input</code> vcf and write each row group in a parquet file …","Read <code>input</code> vcf and write parquet in <code>output</code>","Arrow error","Contains the error value","","Io error","Not support type conversion","Noodles header vcf error","Contains the success value","Parquet error","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","","","","","","","","Alias of std::collections::HashMap that associate a column …","","Add a vcf record in std::collections::HashMap struct","","","","","","","Returns the argument unchanged.","Returns the argument unchanged.","Just a wrapper arround std::collections::HashMap::get","Just a wrapper arround std::collections::HashMap::get_mut","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Convert Name2Data in vector of arrow2 array","Convert ColumnData in Arrow2 array","Create a new Name2Data, vcf header is required to add info …","Add a boolean value in array, if it’s not a boolean …","Add a f32 value in array, if it’s not a float array …","Add a i32 value in array, if it’s not a integer array …","Add a Null value in array","Add a string value in array, if it’s not a string array …","Add a vector of bool value in array, if it’s not a …","Add a vector of float value in array, if it’s not a …","Add a vector of integer value in array, if it’s not a …","Add a vector of string value in array, if it’s not a …","","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","Generate a parquet schema corresponding to vcf header"],"i":[0,0,0,0,0,0,6,34,0,6,6,6,34,6,0,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,23,0,23,23,23,23,23,23,0,23,20,20,23,20,23,20,23,20,23,20,20,20,23,20,23,20,23,23,23,23,23,23,23,23,23,20,23,20,23,20,23,0,30,30,30,30,30,30,30,30,30,30,30,0],"f":[0,0,0,0,[[-1,1,2,3,4],[[7,[5,6]]],8],[[-1,-2,2,3,4],[[7,[5,6]]],8,9],0,0,0,0,0,0,0,0,0,[-1,-2,[],[]],[-1,-2,[],[]],[[6,10],[[7,[5,11]]]],[[6,10],[[7,[5,11]]]],[12,6],[13,6],[14,6],[15,6],[-1,-1,[]],[-1,-2,[],[]],[6,[[17,[16]]]],[-1,18,[]],[-1,[[7,[-2]]],[],[]],[-1,[[7,[-2]]],[],[]],[-1,19,[]],0,0,0,0,0,0,0,0,0,0,[[20,21,22],[[7,[5,12]]]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[[20,10],[[7,[5,11]]]],[[23,10],[[7,[5,11]]]],[-1,-1,[]],[-1,-1,[]],[[20,1],[[17,[23]]]],[[20,1],[[17,[23]]]],[-1,-2,[],[]],[-1,-2,[],[]],[[20,24],[[27,[[26,[25]]]]]],[23,[[26,[25]]]],[[2,22],20],[[23,[17,[4]]],5],[[23,[17,[28]]],5],[[23,[17,[29]]],5],[23,5],[[23,18],5],[[23,[27,[[17,[4]]]]],[[7,[5,12]]]],[[23,[27,[[17,[28]]]]],[[7,[5,12]]]],[[23,[27,[[17,[29]]]]],[[7,[5,12]]]],[[23,[27,[[17,[18]]]]],[[7,[5,12]]]],[-1,[[7,[-2]]],[],[]],[-1,[[7,[-2]]],[],[]],[-1,[[7,[-2]]],[],[]],[-1,[[7,[-2]]],[],[]],[-1,19,[]],[-1,19,[]],0,[-1,-2,[],[]],[-1,-2,[],[]],[[[30,[-1]]],[[27,[[27,[31]]]]],[[33,[],[[32,[[7,[21,15]]]]]]]],[-1,-1,[]],[-1,-2,[],[]],[-1,-2,[],[]],[[-1,2,22,24],[[30,[-1]]],[[33,[],[[32,[[7,[21,15]]]]]]]],[[[30,[-1]]],17,[[33,[],[[32,[[7,[21,15]]]]]]]],[-1,[[7,[-2]]],[],[]],[-1,[[7,[-2]]],[],[]],[-1,19,[]],[[22,4],[[7,[24,6]]]]],"c":[],"p":[[1,"str"],[1,"usize"],[6,"CompressionOptions",84],[1,"bool"],[1,"tuple"],[6,"Error",6],[6,"Result",85],[10,"BufRead",86],[10,"Write",86],[5,"Formatter",87],[5,"Error",87],[6,"Error",88],[6,"Error",89],[6,"ParseError",90],[5,"Error",91],[10,"Error",92],[6,"Option",93],[5,"String",94],[5,"TypeId",95],[5,"Name2Data",30],[5,"Record",96],[5,"Header",97],[6,"ColumnData",30],[5,"Schema",98],[10,"Array",99],[5,"Arc",100],[5,"Vec",101],[1,"f32"],[1,"i32"],[5,"Record2Chunk",71],[6,"Encoding",84],[17,"Item"],[10,"Iterator",102],[8,"Result",6]],"b":[[17,"impl-Debug-for-Error"],[18,"impl-Display-for-Error"],[19,"impl-From%3CError%3E-for-Error"],[20,"impl-From%3CError%3E-for-Error"],[21,"impl-From%3CParseError%3E-for-Error"],[22,"impl-From%3CError%3E-for-Error"]]}],\
["vcf2parquet_bin",{"doc":"vcf2parquet bin","t":"CCHPFGFPPPPPFPGPPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNPGPPPPINNNNNNNNNHNNNNNOOO","n":["cli","error","main","Brotli","Command","Compression","Convert","Convert","Gzip","Lz4","Lzo","Snappy","Split","Split","SubCommand","Uncompressed","Zstd","__clone_box","__clone_box","__clone_box","__clone_box","augment_args","augment_args","augment_args","augment_args_for_update","augment_args_for_update","augment_args_for_update","augment_subcommands","augment_subcommands_for_update","batch_size","borrow","borrow","borrow","borrow","borrow","borrow_mut","borrow_mut","borrow_mut","borrow_mut","borrow_mut","clone","clone","clone","clone","clone_into","clone_into","clone_into","clone_into","command","command","command","command","command_for_update","command_for_update","command_for_update","command_for_update","compression","fmt","fmt","fmt","fmt","fmt","format","from","from","from","from","from","from_arg_matches","from_arg_matches","from_arg_matches","from_arg_matches","from_arg_matches_mut","from_arg_matches_mut","from_arg_matches_mut","from_arg_matches_mut","group_id","group_id","group_id","has_subcommand","info_optional","input","into","into","into","into","into","output","read_buffer","subcommand","to_owned","to_owned","to_owned","to_owned","to_possible_value","try_from","try_from","try_from","try_from","try_from","try_into","try_into","try_into","try_into","try_into","type_id","type_id","type_id","type_id","type_id","update_from_arg_matches","update_from_arg_matches","update_from_arg_matches","update_from_arg_matches","update_from_arg_matches_mut","update_from_arg_matches_mut","update_from_arg_matches_mut","update_from_arg_matches_mut","value_variants","Err","Error","Io","Lib","Niffler","Ok","Result","borrow","borrow_mut","fmt","fmt","from","from","from","from","into","mapping","source","to_string","try_from","try_into","type_id","error","error","error"],"q":[[0,"vcf2parquet_bin"],[3,"vcf2parquet_bin::cli"],[119,"vcf2parquet_bin::error"],[141,"vcf2parquet_bin::error::Error"],[144,"dyn_clone::sealed"],[145,"clap_builder::builder::command"],[146,"parquet2::parquet_bridge"],[147,"core::fmt"],[148,"core::fmt"],[149,"clap_builder"],[150,"core::result"],[151,"clap_builder::util::id"],[152,"core::option"],[153,"std::path"],[154,"clap_builder::builder::possible_value"],[155,"core::any"],[156,"std::io::error"],[157,"niffler::error"],[158,"vcf2parquet_lib::error"],[159,"core::convert"],[160,"core::error"],[161,"alloc::string"]],"d":["cli of vcf2parquet-bin","error of vcf2parquet-bin","","","","","Convert a vcf in a parquet","","","","","","Convert a vcf in multiple parquet file each file contains …","","","","","","","","","","","","","","","","","Get batch_size set by user or default value","","","","","","","","","","","","","","","","","","","","","","","","","","","Get compression set by user or default value","","","","","","Get output format","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","Returns the argument unchanged.","","","","","","","","","","","","","Get info optional","Get input","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Get output","Get read buffer size","Get subcommand","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","Contains the error value","","Io error","vcf2parquet-lib error","Niffler error","Contains the success value","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","","",""],"i":[0,0,0,7,0,0,0,8,7,7,7,7,0,8,0,7,7,7,8,9,10,5,9,10,5,9,10,8,8,5,7,5,8,9,10,7,5,8,9,10,7,8,9,10,7,8,9,10,5,8,9,10,5,8,9,10,5,7,5,8,9,10,10,7,5,8,9,10,5,8,9,10,5,8,9,10,5,9,10,8,5,5,7,5,8,9,10,9,5,5,7,8,9,10,7,7,5,8,9,10,7,5,8,9,10,7,5,8,9,10,5,8,9,10,5,8,9,10,7,2,0,25,25,25,2,0,25,25,25,25,25,25,25,25,25,0,25,25,25,25,25,32,33,34],"f":[0,0,[[],[[2,[1]]]],0,0,0,0,0,0,0,0,0,0,0,0,0,0,[[-1,3],1,[]],[[-1,3],1,[]],[[-1,3],1,[]],[[-1,3],1,[]],[4,4],[4,4],[4,4],[4,4],[4,4],[4,4],[4,4],[4,4],[5,6],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[7,7],[8,8],[9,9],[10,10],[[-1,-2],1,[],[]],[[-1,-2],1,[],[]],[[-1,-2],1,[],[]],[[-1,-2],1,[],[]],[[],4],[[],4],[[],4],[[],4],[[],4],[[],4],[[],4],[[],4],[5,11],[[7,12],13],[[5,12],13],[[8,12],13],[[9,12],13],[[10,12],13],[10,14],[-1,-1,[]],[-1,-1,[]],[-1,-1,[]],[-1,-1,[]],[-1,-1,[]],[15,[[17,[5,16]]]],[15,[[17,[8,16]]]],[15,[[17,[9,16]]]],[15,[[17,[10,16]]]],[15,[[17,[5,16]]]],[15,[[17,[8,16]]]],[15,[[17,[9,16]]]],[15,[[17,[10,16]]]],[[],[[19,[18]]]],[[],[[19,[18]]]],[[],[[19,[18]]]],[14,20],[5,20],[5,21],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[9,21],[5,6],[5,8],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[7,[[19,[22]]]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,23,[]],[-1,23,[]],[-1,23,[]],[-1,23,[]],[-1,23,[]],[[5,15],[[17,[1,16]]]],[[8,15],[[17,[1,16]]]],[[9,15],[[17,[1,16]]]],[[10,15],[[17,[1,16]]]],[[5,15],[[17,[1,16]]]],[[8,15],[[17,[1,16]]]],[[9,15],[[17,[1,16]]]],[[10,15],[[17,[1,16]]]],[[],[[24,[7]]]],0,0,0,0,0,0,0,[-1,-2,[],[]],[-1,-2,[],[]],[[25,12],13],[[25,12],13],[26,25],[27,25],[28,25],[-1,-1,[]],[-1,-2,[],[]],[-1,25,[[29,[25]]]],[25,[[19,[30]]]],[-1,31,[]],[-1,[[17,[-2]]],[],[]],[-1,[[17,[-2]]],[],[]],[-1,23,[]],0,0,0],"c":[],"p":[[1,"tuple"],[8,"Result",119],[5,"Private",144],[5,"Command",145],[5,"Command",3],[1,"usize"],[6,"Compression",3],[6,"SubCommand",3],[5,"Convert",3],[5,"Split",3],[6,"CompressionOptions",146],[5,"Formatter",147],[8,"Result",147],[1,"str"],[5,"ArgMatches",148],[8,"Error",149],[6,"Result",150],[5,"Id",151],[6,"Option",152],[1,"bool"],[5,"PathBuf",153],[5,"PossibleValue",154],[5,"TypeId",155],[1,"slice"],[6,"Error",119],[5,"Error",156],[6,"Error",157],[6,"Error",158],[10,"Into",159],[10,"Error",160],[5,"String",161],[15,"Io",141],[15,"Niffler",141],[15,"Lib",141]],"b":[[128,"impl-Debug-for-Error"],[129,"impl-Display-for-Error"],[130,"impl-From%3CError%3E-for-Error"],[131,"impl-From%3CError%3E-for-Error"],[132,"impl-From%3CError%3E-for-Error"]]}],\
["vcf2parquet_lib",{"doc":"vcf2parquet library","t":"CCCCHHPPGPPPPPINNNNNNNNNNNNNNNPGPPPPPPFPNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNFNNNNNNNNNNNH","n":["error","name2data","record2chunk","schema","vcf2multiparquet","vcf2parquet","Arrow","Err","Error","Io","NoConversion","NoodlesHeader","Ok","Parquet","Result","borrow","borrow_mut","fmt","fmt","from","from","from","from","from","into","source","to_string","try_from","try_into","type_id","Bool","ColumnData","Float","Int","ListBool","ListFloat","ListInt","ListString","Name2Data","String","add_record","borrow","borrow","borrow_mut","borrow_mut","fmt","fmt","from","from","get","get_mut","into","into","into_arc","into_arc","new","push_bool","push_f32","push_i32","push_null","push_string","push_vecbool","push_vecf32","push_veci32","push_vecstring","try_from","try_from","try_into","try_into","type_id","type_id","Record2Chunk","borrow","borrow_mut","encodings","from","into","into_iter","new","next","try_from","try_into","type_id","from_header"],"q":[[0,"vcf2parquet_lib"],[6,"vcf2parquet_lib::error"],[30,"vcf2parquet_lib::name2data"],[71,"vcf2parquet_lib::record2chunk"],[83,"vcf2parquet_lib::schema"],[84,"parquet2::parquet_bridge"],[85,"std::io"],[86,"std::io"],[87,"core::fmt"],[88,"arrow2::error"],[89,"std::io::error"],[90,"parquet2::error"],[91,"core::error"],[92,"core::option"],[93,"alloc::string"],[94,"core::result"],[95,"core::any"],[96,"noodles_vcf::record"],[97,"noodles_vcf::header"],[98,"arrow2::datatypes::schema"],[99,"arrow2::array"],[100,"alloc::sync"],[101,"alloc::vec"],[102,"arrow2::error"]],"d":["vcf2parquet error","Struct to link name and data","Convert vcf record iterator into Parquet chunk","Construct parquet schema corresponding to vcf","Read <code>input</code> vcf and write each row group in a parquet file …","Read <code>input</code> vcf and write parquet in <code>output</code>","Arrow error","Contains the error value","","Io error","Not support type conversion","Noodles header vcf error","Contains the success value","Parquet error","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","","","","","","","","Alias of std::collections::HashMap that associate a column …","","Add a vcf record in std::collections::HashMap struct","","","","","","","Returns the argument unchanged.","Returns the argument unchanged.","Just a wrapper arround std::collections::HashMap::get","Just a wrapper arround std::collections::HashMap::get_mut","Calls <code>U::from(self)</code>.","Calls <code>U::from(self)</code>.","Convert Name2Data in vector of arrow2 array","Convert ColumnData in Arrow2 array","Create a new Name2Data, vcf header is required to add info …","Add a boolean value in array, if it’s not a boolean …","Add a f32 value in array, if it’s not a float array …","Add a i32 value in array, if it’s not a integer array …","Add a Null value in array","Add a string value in array, if it’s not a string array …","Add a vector of bool value in array, if it’s not a …","Add a vector of float value in array, if it’s not a …","Add a vector of integer value in array, if it’s not a …","Add a vector of string value in array, if it’s not a …","","","","","","","","","","","Returns the argument unchanged.","Calls <code>U::from(self)</code>.","","","","","","","Generate a parquet schema corresponding to vcf header"],"i":[0,0,0,0,0,0,9,6,0,9,9,9,6,9,0,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,24,0,24,24,24,24,24,24,0,24,21,21,24,21,24,21,24,21,24,21,21,21,24,21,24,21,24,24,24,24,24,24,24,24,24,21,24,21,24,21,24,0,32,32,32,32,32,32,32,32,32,32,32,0],"f":[0,0,0,0,[[-1,1,2,3,4],[[6,[5]]],7],[[-1,-2,2,3,4],[[6,[5]]],7,8],0,0,0,0,0,0,0,0,0,[-1,-2,[],[]],[-1,-2,[],[]],[[9,10],11],[[9,10],11],[12,9],[13,9],[14,9],[15,9],[-1,-1,[]],[-1,-2,[],[]],[9,[[17,[16]]]],[-1,18,[]],[-1,[[19,[-2]]],[],[]],[-1,[[19,[-2]]],[],[]],[-1,20,[]],0,0,0,0,0,0,0,0,0,0,[[21,22,23],[[19,[5,13]]]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[-1,-2,[],[]],[[21,10],11],[[24,10],11],[-1,-1,[]],[-1,-1,[]],[[21,1],[[17,[24]]]],[[21,1],[[17,[24]]]],[-1,-2,[],[]],[-1,-2,[],[]],[[21,25],[[28,[[27,[26]]]]]],[24,[[27,[26]]]],[[2,23],21],[[24,[17,[4]]],5],[[24,[17,[29]]],5],[[24,[17,[30]]],5],[24,5],[[24,18],5],[[24,[28,[[17,[4]]]]],[[31,[5]]]],[[24,[28,[[17,[29]]]]],[[31,[5]]]],[[24,[28,[[17,[30]]]]],[[31,[5]]]],[[24,[28,[[17,[18]]]]],[[31,[5]]]],[-1,[[19,[-2]]],[],[]],[-1,[[19,[-2]]],[],[]],[-1,[[19,[-2]]],[],[]],[-1,[[19,[-2]]],[],[]],[-1,20,[]],[-1,20,[]],0,[-1,-2,[],[]],[-1,-2,[],[]],[[[32,[-1]]],[[28,[[28,[33]]]]],[[36,[],[[34,[[35,[22]]]]]]]],[-1,-1,[]],[-1,-2,[],[]],[-1,-2,[],[]],[[-1,2,23,25],[[32,[-1]]],[[36,[],[[34,[[35,[22]]]]]]]],[[[32,[-1]]],[[17,[-2]]],[[36,[],[[34,[[35,[22]]]]]]],[]],[-1,[[19,[-2]]],[],[]],[-1,[[19,[-2]]],[],[]],[-1,20,[]],[[23,4],[[6,[25]]]]],"c":[],"p":[[1,"str"],[1,"usize"],[6,"CompressionOptions",84],[1,"bool"],[1,"tuple"],[8,"Result",6],[10,"BufRead",85],[10,"Write",85],[6,"Error",6],[5,"Formatter",86],[8,"Result",86],[6,"ParseError",87],[6,"Error",88],[5,"Error",89],[6,"Error",90],[10,"Error",91],[6,"Option",92],[5,"String",93],[6,"Result",94],[5,"TypeId",95],[5,"Name2Data",30],[5,"Record",96],[5,"Header",97],[6,"ColumnData",30],[5,"Schema",98],[10,"Array",99],[5,"Arc",100],[5,"Vec",101],[1,"f32"],[1,"i32"],[8,"Result",88],[5,"Record2Chunk",71],[6,"Encoding",84],[17,"Item"],[8,"Result",89],[10,"Iterator",102]],"b":[[17,"impl-Display-for-Error"],[18,"impl-Debug-for-Error"],[19,"impl-From%3CParseError%3E-for-Error"],[20,"impl-From%3CError%3E-for-Error"],[21,"impl-From%3CError%3E-for-Error"],[22,"impl-From%3CParquetError%3E-for-Error"]]}]\
]'));
if (typeof exports !== 'undefined') exports.searchIndex = searchIndex;
else if (window.initSearch) window.initSearch(searchIndex);
