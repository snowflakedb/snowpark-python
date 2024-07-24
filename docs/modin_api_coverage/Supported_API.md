|      | class                   | group                                                 | method                               | coverage   | milestone   |
|------|-------------------------|-------------------------------------------------------|--------------------------------------|------------|-------------|
|    0 | CategoricalIndex        | Categorical components                                | add_categories                       | ❌         |             |
|    1 | CategoricalIndex        | Categorical components                                | as_ordered                           | ❌         |             |
|    2 | CategoricalIndex        | Categorical components                                | as_unordered                         | ❌         |             |
|    3 | CategoricalIndex        | Categorical components                                | categories                           | ✅         |             |
|    4 | CategoricalIndex        | Categorical components                                | codes                                | ✅         |             |
|    5 | CategoricalIndex        | Categorical components                                | ordered                              | ❌         |             |
|    6 | CategoricalIndex        | Categorical components                                | remove_categories                    | ❌         |             |
|    7 | CategoricalIndex        | Categorical components                                | remove_unused_categories             | ❌         |             |
|    8 | CategoricalIndex        | Categorical components                                | rename_categories                    | ❌         |             |
|    9 | CategoricalIndex        | Categorical components                                | reorder_categories                   | ❌         |             |
|   10 | CategoricalIndex        | Categorical components                                | set_categories                       | ❌         |             |
|   11 | CategoricalIndex        | Modifying and computations                            | equals                               | ❌         |             |
|   12 | CategoricalIndex        | Modifying and computations                            | map                                  | ❌         |             |
|   13 | DataFrame               | Attributes and underlying data                        | axes                                 | ✅         |             |
|   14 | DataFrame               | Attributes and underlying data                        | columns                              | ✅         | phase 1     |
|   15 | DataFrame               | Attributes and underlying data                        | dtypes                               | ✅         |             |
|   16 | DataFrame               | Attributes and underlying data                        | empty                                | ✅         | phase 1     |
|   17 | DataFrame               | Attributes and underlying data                        | index                                | ✅         | phase 1     |
|   18 | DataFrame               | Attributes and underlying data                        | info                                 | ✅         | phase 2     |
|   19 | DataFrame               | Attributes and underlying data                        | memory_usage                         | ✅         |             |
|   20 | DataFrame               | Attributes and underlying data                        | ndim                                 | ✅         |             |
|   21 | DataFrame               | Attributes and underlying data                        | select_dtypes                        | ✅         | phase 2     |
|   22 | DataFrame               | Attributes and underlying data                        | set_flags                            | ❌         |             |
|   23 | DataFrame               | Attributes and underlying data                        | shape                                | ✅         | phase 1     |
|   24 | DataFrame               | Attributes and underlying data                        | size                                 | ✅         | phase 1     |
|   25 | DataFrame               | Attributes and underlying data                        | values                               | ✅         | phase 1     |
|   26 | DataFrame               | Binary operator functions                             | __add__                              | ✅         |             |
|   27 | DataFrame               | Binary operator functions                             | add                                  | 🟡         | phase 1     |
|   28 | DataFrame               | Binary operator functions                             | combine                              | ❌         |             |
|   29 | DataFrame               | Binary operator functions                             | combine_first                        | ❌         |             |
|   30 | DataFrame               | Binary operator functions                             | div                                  | ✅         |             |
|   31 | DataFrame               | Binary operator functions                             | dot                                  | ❌         |             |
|   32 | DataFrame               | Binary operator functions                             | eq                                   | ✅         |             |
|   33 | DataFrame               | Binary operator functions                             | floordiv                             | ✅         |             |
|   34 | DataFrame               | Binary operator functions                             | ge                                   | ✅         |             |
|   35 | DataFrame               | Binary operator functions                             | gt                                   | ✅         |             |
|   36 | DataFrame               | Binary operator functions                             | le                                   | ✅         |             |
|   37 | DataFrame               | Binary operator functions                             | lt                                   | ✅         |             |
|   38 | DataFrame               | Binary operator functions                             | mod                                  | ✅         |             |
|   39 | DataFrame               | Binary operator functions                             | mul                                  | ✅         |             |
|   40 | DataFrame               | Binary operator functions                             | ne                                   | ✅         |             |
|   41 | DataFrame               | Binary operator functions                             | pow                                  | ✅         |             |
|   42 | DataFrame               | Binary operator functions                             | radd                                 | ✅         |             |
|   43 | DataFrame               | Binary operator functions                             | rdiv                                 | ✅         |             |
|   44 | DataFrame               | Binary operator functions                             | rfloordiv                            | ✅         |             |
|   45 | DataFrame               | Binary operator functions                             | rmod                                 | ✅         |             |
|   46 | DataFrame               | Binary operator functions                             | rmul                                 | ✅         |             |
|   47 | DataFrame               | Binary operator functions                             | rpow                                 | ✅         |             |
|   48 | DataFrame               | Binary operator functions                             | rsub                                 | ✅         |             |
|   49 | DataFrame               | Binary operator functions                             | rtruediv                             | ✅         |             |
|   50 | DataFrame               | Binary operator functions                             | sub                                  | ✅         | phase 1     |
|   51 | DataFrame               | Binary operator functions                             | truediv                              | ✅         |             |
|   52 | DataFrame               | Combining / comparing / joining / merging             | assign                               | ✅         |             |
|   53 | DataFrame               | Combining / comparing / joining / merging             | compare                              | ❌         |             |
|   54 | DataFrame               | Combining / comparing / joining / merging             | join                                 | 🟡         | phase 1     |
|   55 | DataFrame               | Combining / comparing / joining / merging             | merge                                | 🟡         | phase 1     |
|   56 | DataFrame               | Combining / comparing / joining / merging             | update                               | ❌         |             |
|   57 | DataFrame               | Computations / descriptive stats                      | abs                                  | ✅         |             |
|   58 | DataFrame               | Computations / descriptive stats                      | all                                  | 🟡         |             |
|   59 | DataFrame               | Computations / descriptive stats                      | any                                  | 🟡         |             |
|   60 | DataFrame               | Computations / descriptive stats                      | clip                                 | ❌         |             |
|   61 | DataFrame               | Computations / descriptive stats                      | corr                                 | 🟡         | phase 2     |
|   62 | DataFrame               | Computations / descriptive stats                      | corrwith                             | ❌         |             |
|   63 | DataFrame               | Computations / descriptive stats                      | count                                | ✅         | phase 1     |
|   64 | DataFrame               | Computations / descriptive stats                      | cov                                  | ❌         |             |
|   65 | DataFrame               | Computations / descriptive stats                      | cummax                               | 🟡         | phase 2     |
|   66 | DataFrame               | Computations / descriptive stats                      | cummin                               | 🟡         | phase 2     |
|   67 | DataFrame               | Computations / descriptive stats                      | cumprod                              | ❌         |             |
|   68 | DataFrame               | Computations / descriptive stats                      | cumsum                               | 🟡         | phase 2     |
|   69 | DataFrame               | Computations / descriptive stats                      | describe                             | ✅         | phase 2     |
|   70 | DataFrame               | Computations / descriptive stats                      | diff                                 | ✅         | phase 2     |
|   71 | DataFrame               | Computations / descriptive stats                      | eval                                 | ❌         |             |
|   72 | DataFrame               | Computations / descriptive stats                      | kurt                                 | ❌         |             |
|   73 | DataFrame               | Computations / descriptive stats                      | kurtosis                             | ❌         |             |
|   74 | DataFrame               | Computations / descriptive stats                      | max                                  | 🟡         | phase 1     |
|   75 | DataFrame               | Computations / descriptive stats                      | mean                                 | ✅         | phase 1     |
|   76 | DataFrame               | Computations / descriptive stats                      | median                               | ✅         | phase 1     |
|   77 | DataFrame               | Computations / descriptive stats                      | min                                  | ✅         | phase 1     |
|   78 | DataFrame               | Computations / descriptive stats                      | mode                                 | ❌         |             |
|   79 | DataFrame               | Computations / descriptive stats                      | nunique                              | 🟡         | phase 1     |
|   80 | DataFrame               | Computations / descriptive stats                      | pct_change                           | 🟡         |             |
|   81 | DataFrame               | Computations / descriptive stats                      | prod                                 | ❌         |             |
|   82 | DataFrame               | Computations / descriptive stats                      | product                              | ❌         |             |
|   83 | DataFrame               | Computations / descriptive stats                      | quantile                             | 🟡         | phase 1     |
|   84 | DataFrame               | Computations / descriptive stats                      | rank                                 | 🟡         | phase 2     |
|   85 | DataFrame               | Computations / descriptive stats                      | round                                | 🟡         | phase 2     |
|   86 | DataFrame               | Computations / descriptive stats                      | sem                                  | ❌         |             |
|   87 | DataFrame               | Computations / descriptive stats                      | skew                                 | 🟡         | phase 2     |
|   88 | DataFrame               | Computations / descriptive stats                      | std                                  | 🟡         | phase 1     |
|   89 | DataFrame               | Computations / descriptive stats                      | sum                                  | ✅         | phase 1     |
|   90 | DataFrame               | Computations / descriptive stats                      | value_counts                         | 🟡         | phase 2     |
|   91 | DataFrame               | Computations / descriptive stats                      | var                                  | 🟡         | phase 1     |
|   92 | DataFrame               | Conversion                                            | astype                               | ✅         | phase 1     |
|   93 | DataFrame               | Conversion                                            | bool                                 | ❌         |             |
|   94 | DataFrame               | Conversion                                            | convert_dtypes                       | ❌         |             |
|   95 | DataFrame               | Conversion                                            | copy                                 | ✅         | phase 1     |
|   96 | DataFrame               | Conversion                                            | infer_objects                        | ❌         |             |
|   97 | DataFrame               | Function application, GroupBy & window                | agg                                  | 🟡         | phase 1     |
|   98 | DataFrame               | Function application, GroupBy & window                | aggregate                            | 🟡         |             |
|   99 | DataFrame               | Function application, GroupBy & window                | apply                                | 🟡         | phase 1     |
|  100 | DataFrame               | Function application, GroupBy & window                | applymap                             | 🟡         | phase 1     |
|  101 | DataFrame               | Function application, GroupBy & window                | ewm                                  | ❌         |             |
|  102 | DataFrame               | Function application, GroupBy & window                | expanding                            | ✅         |             |
|  103 | DataFrame               | Function application, GroupBy & window                | groupby                              | ✅         | phase 1     |
|  104 | DataFrame               | Function application, GroupBy & window                | pipe                                 | ❌         |             |
|  105 | DataFrame               | Function application, GroupBy & window                | rolling                              | ✅         | phase 2     |
|  106 | DataFrame               | Function application, GroupBy & window                | transform                            | ❌         |             |
|  107 | DataFrame               | Indexing, iteration                                   | __iter__                             | ✅         |             |
|  108 | DataFrame               | Indexing, iteration                                   | at                                   | ✅         |             |
|  109 | DataFrame               | Indexing, iteration                                   | get                                  | ❌         | on-hold     |
|  110 | DataFrame               | Indexing, iteration                                   | head                                 | ✅         | phase 1     |
|  111 | DataFrame               | Indexing, iteration                                   | iat                                  | ✅         |             |
|  112 | DataFrame               | Indexing, iteration                                   | iloc                                 | ✅         | phase 1     |
|  113 | DataFrame               | Indexing, iteration                                   | insert                               | ✅         | phase 1     |
|  114 | DataFrame               | Indexing, iteration                                   | isin                                 | ✅         | phase 2     |
|  115 | DataFrame               | Indexing, iteration                                   | items                                | ❌         | on-hold     |
|  116 | DataFrame               | Indexing, iteration                                   | iterrows                             | ✅         | phase 2     |
|  117 | DataFrame               | Indexing, iteration                                   | itertuples                           | ✅         | phase 2     |
|  118 | DataFrame               | Indexing, iteration                                   | keys                                 | ❌         | on-hold     |
|  119 | DataFrame               | Indexing, iteration                                   | loc                                  | ✅         | phase 1     |
|  120 | DataFrame               | Indexing, iteration                                   | mask                                 | 🟡         |             |
|  121 | DataFrame               | Indexing, iteration                                   | pop                                  | ❌         |             |
|  122 | DataFrame               | Indexing, iteration                                   | query                                | ❌         |             |
|  123 | DataFrame               | Indexing, iteration                                   | tail                                 | ✅         | phase 1     |
|  124 | DataFrame               | Indexing, iteration                                   | where                                | 🟡         | phase 1     |
|  125 | DataFrame               | Indexing, iteration                                   | xs                                   | ❌         |             |
|  126 | DataFrame               | Metadata                                              | attrs                                | ❌         |             |
|  127 | DataFrame               | Missing data handling                                 | backfill                             | ❌         |             |
|  128 | DataFrame               | Missing data handling                                 | bfill                                | ❌         |             |
|  129 | DataFrame               | Missing data handling                                 | dropna                               | 🟡         | phase 1     |
|  130 | DataFrame               | Missing data handling                                 | ffill                                | ✅         |             |
|  131 | DataFrame               | Missing data handling                                 | fillna                               | 🟡         | phase 1     |
|  132 | DataFrame               | Missing data handling                                 | interpolate                          | ❌         |             |
|  133 | DataFrame               | Missing data handling                                 | isna                                 | ✅         | phase 1     |
|  134 | DataFrame               | Missing data handling                                 | isnull                               | ✅         | phase 1     |
|  135 | DataFrame               | Missing data handling                                 | notna                                | ✅         | phase 1     |
|  136 | DataFrame               | Missing data handling                                 | notnull                              | ✅         | phase 1     |
|  137 | DataFrame               | Missing data handling                                 | pad                                  | ✅         |             |
|  138 | DataFrame               | Missing data handling                                 | replace                              | 🟡         | phase 2     |
|  139 | DataFrame               | Plotting                                              | boxplot                              | ❌         |             |
|  140 | DataFrame               | Plotting                                              | hist                                 | ❌         |             |
|  141 | DataFrame               | Plotting                                              | plot                                 | ❌         |             |
|  142 | DataFrame               | Plotting                                              | plot.area                            | ❌         |             |
|  143 | DataFrame               | Plotting                                              | plot.bar                             | ❌         |             |
|  144 | DataFrame               | Plotting                                              | plot.barh                            | ❌         |             |
|  145 | DataFrame               | Plotting                                              | plot.box                             | ❌         |             |
|  146 | DataFrame               | Plotting                                              | plot.density                         | ❌         |             |
|  147 | DataFrame               | Plotting                                              | plot.hexbin                          | ❌         |             |
|  148 | DataFrame               | Plotting                                              | plot.hist                            | ❌         |             |
|  149 | DataFrame               | Plotting                                              | plot.kde                             | ❌         |             |
|  150 | DataFrame               | Plotting                                              | plot.line                            | ❌         |             |
|  151 | DataFrame               | Plotting                                              | plot.pie                             | ❌         |             |
|  152 | DataFrame               | Plotting                                              | plot.scatter                         | ❌         |             |
|  153 | DataFrame               | Plotting and visualization                            | boxplot                              | ❌         |             |
|  154 | DataFrame               | Plotting and visualization                            | hist                                 | ❌         |             |
|  155 | DataFrame               | Plotting and visualization                            | plot                                 | ❌         |             |
|  156 | DataFrame               | Plotting and visualization                            | plot.area                            | ❌         |             |
|  157 | DataFrame               | Plotting and visualization                            | plot.bar                             | ❌         |             |
|  158 | DataFrame               | Plotting and visualization                            | plot.barh                            | ❌         |             |
|  159 | DataFrame               | Plotting and visualization                            | plot.box                             | ❌         |             |
|  160 | DataFrame               | Plotting and visualization                            | plot.density                         | ❌         |             |
|  161 | DataFrame               | Plotting and visualization                            | plot.hexbin                          | ❌         |             |
|  162 | DataFrame               | Plotting and visualization                            | plot.hist                            | ❌         |             |
|  163 | DataFrame               | Plotting and visualization                            | plot.kde                             | ❌         |             |
|  164 | DataFrame               | Plotting and visualization                            | plot.line                            | ❌         |             |
|  165 | DataFrame               | Plotting and visualization                            | plot.pie                             | ❌         |             |
|  166 | DataFrame               | Plotting and visualization                            | plot.scatter                         | ❌         |             |
|  167 | DataFrame               | Reindexing / selection / label manipulation           | add_prefix                           | ✅         | phase 2     |
|  168 | DataFrame               | Reindexing / selection / label manipulation           | add_suffix                           | ✅         | phase 2     |
|  169 | DataFrame               | Reindexing / selection / label manipulation           | align                                | ❌         |             |
|  170 | DataFrame               | Reindexing / selection / label manipulation           | at_time                              | ❌         |             |
|  171 | DataFrame               | Reindexing / selection / label manipulation           | between_time                         | ❌         |             |
|  172 | DataFrame               | Reindexing / selection / label manipulation           | drop                                 | ✅         | phase 1     |
|  173 | DataFrame               | Reindexing / selection / label manipulation           | drop_duplicates                      | ✅         | phase 2     |
|  174 | DataFrame               | Reindexing / selection / label manipulation           | duplicated                           | ✅         | phase 2     |
|  175 | DataFrame               | Reindexing / selection / label manipulation           | equals                               | ❌         |             |
|  176 | DataFrame               | Reindexing / selection / label manipulation           | filter                               | ❌         | on-hold     |
|  177 | DataFrame               | Reindexing / selection / label manipulation           | first                                | ❌         | on-hold     |
|  178 | DataFrame               | Reindexing / selection / label manipulation           | head                                 | ✅         | phase 1     |
|  179 | DataFrame               | Reindexing / selection / label manipulation           | idxmax                               | 🟡         | phase 2     |
|  180 | DataFrame               | Reindexing / selection / label manipulation           | idxmin                               | 🟡         | phase 2     |
|  181 | DataFrame               | Reindexing / selection / label manipulation           | last                                 | ❌         |             |
|  182 | DataFrame               | Reindexing / selection / label manipulation           | reindex                              | ❌         |             |
|  183 | DataFrame               | Reindexing / selection / label manipulation           | reindex_like                         | ❌         |             |
|  184 | DataFrame               | Reindexing / selection / label manipulation           | rename                               | 🟡         | phase 1     |
|  185 | DataFrame               | Reindexing / selection / label manipulation           | rename_axis                          | ✅         |             |
|  186 | DataFrame               | Reindexing / selection / label manipulation           | reset_index                          | ✅         | phase 1     |
|  187 | DataFrame               | Reindexing / selection / label manipulation           | sample                               | ✅         | phase 2     |
|  188 | DataFrame               | Reindexing / selection / label manipulation           | set_axis                             | ✅         |             |
|  189 | DataFrame               | Reindexing / selection / label manipulation           | set_index                            | ✅         | phase 1     |
|  190 | DataFrame               | Reindexing / selection / label manipulation           | tail                                 | ✅         | phase 1     |
|  191 | DataFrame               | Reindexing / selection / label manipulation           | take                                 | ✅         |             |
|  192 | DataFrame               | Reindexing / selection / label manipulation           | truncate                             | ❌         |             |
|  193 | DataFrame               | Reshaping, sorting, transposing                       | T                                    | 🟡         | phase 1     |
|  194 | DataFrame               | Reshaping, sorting, transposing                       | droplevel                            | ❌         |             |
|  195 | DataFrame               | Reshaping, sorting, transposing                       | explode                              | ❌         |             |
|  196 | DataFrame               | Reshaping, sorting, transposing                       | melt                                 | 🟡         | phase 2     |
|  197 | DataFrame               | Reshaping, sorting, transposing                       | nlargest                             | 🟡         |             |
|  198 | DataFrame               | Reshaping, sorting, transposing                       | nsmallest                            | 🟡         |             |
|  199 | DataFrame               | Reshaping, sorting, transposing                       | pivot                                | ✅         |             |
|  200 | DataFrame               | Reshaping, sorting, transposing                       | pivot_table                          | 🟡         | phase 1     |
|  201 | DataFrame               | Reshaping, sorting, transposing                       | reorder_levels                       | ❌         |             |
|  202 | DataFrame               | Reshaping, sorting, transposing                       | sort_index                           | 🟡         | phase 2     |
|  203 | DataFrame               | Reshaping, sorting, transposing                       | sort_values                          | 🟡         | phase 1     |
|  204 | DataFrame               | Reshaping, sorting, transposing                       | squeeze                              | ✅         |             |
|  205 | DataFrame               | Reshaping, sorting, transposing                       | stack                                | 🟡         |             |
|  206 | DataFrame               | Reshaping, sorting, transposing                       | swapaxes                             | ❌         |             |
|  207 | DataFrame               | Reshaping, sorting, transposing                       | swaplevel                            | ❌         |             |
|  208 | DataFrame               | Reshaping, sorting, transposing                       | to_xarray                            | ❌         |             |
|  209 | DataFrame               | Reshaping, sorting, transposing                       | transpose                            | ✅         | phase 1     |
|  210 | DataFrame               | Reshaping, sorting, transposing                       | unstack                              | ❌         |             |
|  211 | DataFrame               | Serialization / IO / conversion                       | __dataframe__                        | ❌         |             |
|  212 | DataFrame               | Serialization / IO / conversion                       | from_dict                            | ❌         |             |
|  213 | DataFrame               | Serialization / IO / conversion                       | from_records                         | ❌         |             |
|  214 | DataFrame               | Serialization / IO / conversion                       | style                                | ❌         |             |
|  215 | DataFrame               | Serialization / IO / conversion                       | to_clipboard                         | ❌         |             |
|  216 | DataFrame               | Serialization / IO / conversion                       | to_csv                               | ❌         |             |
|  217 | DataFrame               | Serialization / IO / conversion                       | to_dict                              | ✅         | phase 2     |
|  218 | DataFrame               | Serialization / IO / conversion                       | to_excel                             | ❌         |             |
|  219 | DataFrame               | Serialization / IO / conversion                       | to_feather                           | ❌         |             |
|  220 | DataFrame               | Serialization / IO / conversion                       | to_gbq                               | ❌         |             |
|  221 | DataFrame               | Serialization / IO / conversion                       | to_hdf                               | ❌         |             |
|  222 | DataFrame               | Serialization / IO / conversion                       | to_html                              | ❌         |             |
|  223 | DataFrame               | Serialization / IO / conversion                       | to_json                              | ❌         |             |
|  224 | DataFrame               | Serialization / IO / conversion                       | to_latex                             | ❌         |             |
|  225 | DataFrame               | Serialization / IO / conversion                       | to_markdown                          | ❌         |             |
|  226 | DataFrame               | Serialization / IO / conversion                       | to_orc                               | ❌         |             |
|  227 | DataFrame               | Serialization / IO / conversion                       | to_parquet                           | ❌         |             |
|  228 | DataFrame               | Serialization / IO / conversion                       | to_pickle                            | ❌         |             |
|  229 | DataFrame               | Serialization / IO / conversion                       | to_records                           | ❌         |             |
|  230 | DataFrame               | Serialization / IO / conversion                       | to_sql                               | ❌         |             |
|  231 | DataFrame               | Serialization / IO / conversion                       | to_stata                             | ❌         |             |
|  232 | DataFrame               | Serialization / IO / conversion                       | to_string                            | ❌         |             |
|  233 | DataFrame               | Sparse accessor                                       | sparse.density                       | ❌         |             |
|  234 | DataFrame               | Sparse accessor                                       | sparse.from_spmatrix                 | ❌         |             |
|  235 | DataFrame               | Sparse accessor                                       | sparse.to_coo                        | ❌         |             |
|  236 | DataFrame               | Sparse accessor                                       | sparse.to_dense                      | ❌         |             |
|  237 | DataFrame               | Time Series-related                                   | asfreq                               | ❌         |             |
|  238 | DataFrame               | Time Series-related                                   | asof                                 | ❌         |             |
|  239 | DataFrame               | Time Series-related                                   | first_valid_index                    | ✅         | phase 2     |
|  240 | DataFrame               | Time Series-related                                   | last_valid_index                     | ✅         | phase 2     |
|  241 | DataFrame               | Time Series-related                                   | resample                             | ✅         | phase 1     |
|  242 | DataFrame               | Time Series-related                                   | shift                                | 🟡         | phase 2     |
|  243 | DataFrame               | Time Series-related                                   | to_period                            | ❌         |             |
|  244 | DataFrame               | Time Series-related                                   | to_timestamp                         | ❌         |             |
|  245 | DataFrame               | Time Series-related                                   | tz_convert                           | ❌         |             |
|  246 | DataFrame               | Time Series-related                                   | tz_localize                          | ❌         |             |
|  247 | DataFrameGroupBy        | Function application                                  | agg                                  | 🟡         |             |
|  248 | DataFrameGroupBy        | Function application                                  | aggregate                            | 🟡         |             |
|  249 | DataFrameGroupBy        | Function application                                  | apply                                | 🟡         | phase 2     |
|  250 | DataFrameGroupBy        | Function application                                  | filter                               | ❌         |             |
|  251 | DataFrameGroupBy        | Function application                                  | pipe                                 | ❌         |             |
|  252 | DataFrameGroupBy        | Function application                                  | transform                            | 🟡         | phase 2     |
|  253 | DataFrameGroupBy        | Indexing, iteration                                   | __iter__                             | ❌         |             |
|  254 | DataFrameGroupBy        | Indexing, iteration                                   | get_group                            | 🟡         |             |
|  255 | DataFrameGroupBy        | Indexing, iteration                                   | groups                               | ✅         |             |
|  256 | DataFrameGroupBy        | Indexing, iteration                                   | indices                              | ✅         |             |
|  257 | DataFrameGroupBy        | Plotting and visualization                            | boxplot                              | ❌         |             |
|  258 | DataFrameGroupBy        | Plotting and visualization                            | hist                                 | ❌         |             |
|  259 | DataFrameGroupBy        | Plotting and visualization                            | plot                                 | ❌         |             |
|  260 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | all                                  | ✅         |             |
|  261 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | any                                  | ✅         |             |
|  262 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | bfill                                | ❌         |             |
|  263 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | corr                                 | ❌         |             |
|  264 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | corrwith                             | ❌         |             |
|  265 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | count                                | ✅         |             |
|  266 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cov                                  | ❌         |             |
|  267 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cumcount                             | 🟡         | phase 2     |
|  268 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cummax                               | ✅         | phase 2     |
|  269 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cummin                               | ✅         | phase 2     |
|  270 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cumprod                              | ❌         |             |
|  271 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cumsum                               | ✅         | phase 2     |
|  272 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | describe                             | ❌         |             |
|  273 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | diff                                 | ❌         |             |
|  274 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | ffill                                | ❌         |             |
|  275 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | fillna                               | ❌         |             |
|  276 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | first                                | 🟡         |             |
|  277 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | head                                 | ✅         | phase 2     |
|  278 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | idxmax                               | 🟡         | phase 2     |
|  279 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | idxmin                               | 🟡         | phase 2     |
|  280 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | last                                 | 🟡         |             |
|  281 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | max                                  | 🟡         |             |
|  282 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | mean                                 | ✅         |             |
|  283 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | median                               | ✅         |             |
|  284 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | min                                  | 🟡         |             |
|  285 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | ngroup                               | ❌         |             |
|  286 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | nth                                  | ❌         |             |
|  287 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | nunique                              | ✅         |             |
|  288 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | ohlc                                 | ❌         |             |
|  289 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | pct_change                           | ❌         |             |
|  290 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | prod                                 | ❌         |             |
|  291 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | quantile                             | 🟡         |             |
|  292 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | rank                                 | 🟡         | phase 2     |
|  293 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | resample                             | ❌         |             |
|  294 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | rolling                              | ❌         |             |
|  295 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | sample                               | ❌         |             |
|  296 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | sem                                  | ❌         |             |
|  297 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | shift                                | 🟡         | phase 2     |
|  298 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | size                                 | 🟡         |             |
|  299 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | skew                                 | ❌         |             |
|  300 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | std                                  | 🟡         |             |
|  301 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | sum                                  | 🟡         |             |
|  302 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | tail                                 | ✅         | phase 2     |
|  303 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | take                                 | ❌         |             |
|  304 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | value_counts                         | ❌         |             |
|  305 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | var                                  | 🟡         |             |
|  306 | DatetimeIndex           | Conversion                                            | as_unit                              | ❌         |             |
|  307 | DatetimeIndex           | Conversion                                            | to_frame                             | ❌         |             |
|  308 | DatetimeIndex           | Conversion                                            | to_period                            | ❌         |             |
|  309 | DatetimeIndex           | Conversion                                            | to_pydatetime                        | ❌         |             |
|  310 | DatetimeIndex           | Conversion                                            | to_series                            | ❌         |             |
|  311 | DatetimeIndex           | Methods                                               | mean                                 | ❌         |             |
|  312 | DatetimeIndex           | Methods                                               | std                                  | ❌         |             |
|  313 | DatetimeIndex           | Selecting                                             | indexer_at_time                      | ❌         |             |
|  314 | DatetimeIndex           | Selecting                                             | indexer_between_time                 | ❌         |             |
|  315 | DatetimeIndex           | Time-specific operations                              | ceil                                 | ❌         |             |
|  316 | DatetimeIndex           | Time-specific operations                              | day_name                             | ❌         |             |
|  317 | DatetimeIndex           | Time-specific operations                              | floor                                | ❌         |             |
|  318 | DatetimeIndex           | Time-specific operations                              | month_name                           | ❌         |             |
|  319 | DatetimeIndex           | Time-specific operations                              | normalize                            | ❌         |             |
|  320 | DatetimeIndex           | Time-specific operations                              | round                                | ❌         |             |
|  321 | DatetimeIndex           | Time-specific operations                              | snap                                 | ❌         |             |
|  322 | DatetimeIndex           | Time-specific operations                              | strftime                             | ❌         |             |
|  323 | DatetimeIndex           | Time-specific operations                              | tz_convert                           | ✅         |             |
|  324 | DatetimeIndex           | Time-specific operations                              | tz_localize                          | ✅         |             |
|  325 | DatetimeIndex           | Time/date components                                  | date                                 | ✅         |             |
|  326 | DatetimeIndex           | Time/date components                                  | day                                  | ✅         |             |
|  327 | DatetimeIndex           | Time/date components                                  | day_of_week                          | ✅         |             |
|  328 | DatetimeIndex           | Time/date components                                  | day_of_year                          | ✅         |             |
|  329 | DatetimeIndex           | Time/date components                                  | dayofweek                            | ✅         |             |
|  330 | DatetimeIndex           | Time/date components                                  | dayofyear                            | ✅         |             |
|  331 | DatetimeIndex           | Time/date components                                  | freq                                 | ✅         |             |
|  332 | DatetimeIndex           | Time/date components                                  | freqstr                              | ✅         |             |
|  333 | DatetimeIndex           | Time/date components                                  | hour                                 | ✅         |             |
|  334 | DatetimeIndex           | Time/date components                                  | inferred_freq                        | ❌         |             |
|  335 | DatetimeIndex           | Time/date components                                  | is_leap_year                         | ❌         |             |
|  336 | DatetimeIndex           | Time/date components                                  | is_month_end                         | ❌         |             |
|  337 | DatetimeIndex           | Time/date components                                  | is_month_start                       | ✅         |             |
|  338 | DatetimeIndex           | Time/date components                                  | is_quarter_end                       | ❌         |             |
|  339 | DatetimeIndex           | Time/date components                                  | is_quarter_start                     | ❌         |             |
|  340 | DatetimeIndex           | Time/date components                                  | is_year_end                          | ✅         |             |
|  341 | DatetimeIndex           | Time/date components                                  | is_year_start                        | ❌         |             |
|  342 | DatetimeIndex           | Time/date components                                  | microsecond                          | ❌         |             |
|  343 | DatetimeIndex           | Time/date components                                  | minute                               | ✅         |             |
|  344 | DatetimeIndex           | Time/date components                                  | month                                | ✅         |             |
|  345 | DatetimeIndex           | Time/date components                                  | nanosecond                           | ❌         |             |
|  346 | DatetimeIndex           | Time/date components                                  | quarter                              | ✅         |             |
|  347 | DatetimeIndex           | Time/date components                                  | second                               | ✅         |             |
|  348 | DatetimeIndex           | Time/date components                                  | time                                 | ❌         |             |
|  349 | DatetimeIndex           | Time/date components                                  | timetz                               | ❌         |             |
|  350 | DatetimeIndex           | Time/date components                                  | tz                                   | ✅         |             |
|  351 | DatetimeIndex           | Time/date components                                  | weekday                              | ✅         |             |
|  352 | DatetimeIndex           | Time/date components                                  | year                                 | ✅         |             |
|  353 | Expanding               | Expanding window functions                            | aggregate                            | ❌         |             |
|  354 | Expanding               | Expanding window functions                            | apply                                | ❌         |             |
|  355 | Expanding               | Expanding window functions                            | corr                                 | ❌         |             |
|  356 | Expanding               | Expanding window functions                            | count                                | ❌         |             |
|  357 | Expanding               | Expanding window functions                            | cov                                  | ❌         |             |
|  358 | Expanding               | Expanding window functions                            | kurt                                 | ❌         |             |
|  359 | Expanding               | Expanding window functions                            | max                                  | ❌         |             |
|  360 | Expanding               | Expanding window functions                            | mean                                 | ❌         |             |
|  361 | Expanding               | Expanding window functions                            | median                               | ❌         |             |
|  362 | Expanding               | Expanding window functions                            | min                                  | ❌         |             |
|  363 | Expanding               | Expanding window functions                            | quantile                             | ❌         |             |
|  364 | Expanding               | Expanding window functions                            | rank                                 | ❌         |             |
|  365 | Expanding               | Expanding window functions                            | sem                                  | ❌         |             |
|  366 | Expanding               | Expanding window functions                            | skew                                 | ❌         |             |
|  367 | Expanding               | Expanding window functions                            | std                                  | ❌         |             |
|  368 | Expanding               | Expanding window functions                            | sum                                  | ❌         |             |
|  369 | Expanding               | Expanding window functions                            | var                                  | ❌         |             |
|  370 | ExponentialMovingWindow | Exponentially-weighted window functions               | corr                                 | ❌         |             |
|  371 | ExponentialMovingWindow | Exponentially-weighted window functions               | cov                                  | ❌         |             |
|  372 | ExponentialMovingWindow | Exponentially-weighted window functions               | mean                                 | ❌         |             |
|  373 | ExponentialMovingWindow | Exponentially-weighted window functions               | std                                  | ❌         |             |
|  374 | ExponentialMovingWindow | Exponentially-weighted window functions               | sum                                  | ❌         |             |
|  375 | ExponentialMovingWindow | Exponentially-weighted window functions               | var                                  | ❌         |             |
|  376 | Index                   | Combining / joining / set operations                  | append                               | ❌         |             |
|  377 | Index                   | Combining / joining / set operations                  | difference                           | ✅         |             |
|  378 | Index                   | Combining / joining / set operations                  | intersection                         | ✅         |             |
|  379 | Index                   | Combining / joining / set operations                  | join                                 | ❌         |             |
|  380 | Index                   | Combining / joining / set operations                  | symmetric_difference                 | ❌         |             |
|  381 | Index                   | Combining / joining / set operations                  | union                                | ✅         |             |
|  382 | Index                   | Compatibility with MultiIndex                         | droplevel                            | ❌         |             |
|  383 | Index                   | Compatibility with MultiIndex                         | set_names                            | ✅         |             |
|  384 | Index                   | Conversion                                            | astype                               | ✅         |             |
|  385 | Index                   | Conversion                                            | item                                 | ✅         |             |
|  386 | Index                   | Conversion                                            | map                                  | ❌         |             |
|  387 | Index                   | Conversion                                            | ravel                                | ❌         |             |
|  388 | Index                   | Conversion                                            | to_frame                             | ✅         |             |
|  389 | Index                   | Conversion                                            | to_list                              | ✅         |             |
|  390 | Index                   | Conversion                                            | to_series                            | ✅         |             |
|  391 | Index                   | Conversion                                            | view                                 | ❌         |             |
|  392 | Index                   | Missing values                                        | dropna                               | ❌         |             |
|  393 | Index                   | Missing values                                        | fillna                               | ❌         |             |
|  394 | Index                   | Missing values                                        | isna                                 | ❌         |             |
|  395 | Index                   | Missing values                                        | notna                                | ❌         |             |
|  396 | Index                   | Modifying and computations                            | all                                  | ❌         |             |
|  397 | Index                   | Modifying and computations                            | any                                  | ❌         |             |
|  398 | Index                   | Modifying and computations                            | argmax                               | ❌         |             |
|  399 | Index                   | Modifying and computations                            | argmin                               | ❌         |             |
|  400 | Index                   | Modifying and computations                            | copy                                 | ✅         |             |
|  401 | Index                   | Modifying and computations                            | delete                               | ❌         |             |
|  402 | Index                   | Modifying and computations                            | drop                                 | ✅         |             |
|  403 | Index                   | Modifying and computations                            | drop_duplicates                      | ❌         |             |
|  404 | Index                   | Modifying and computations                            | duplicated                           | ✅         |             |
|  405 | Index                   | Modifying and computations                            | equals                               | ✅         |             |
|  406 | Index                   | Modifying and computations                            | factorize                            | ❌         |             |
|  407 | Index                   | Modifying and computations                            | identical                            | ❌         |             |
|  408 | Index                   | Modifying and computations                            | insert                               | ❌         |             |
|  409 | Index                   | Modifying and computations                            | is_                                  | ❌         |             |
|  410 | Index                   | Modifying and computations                            | is_boolean                           | ❌         |             |
|  411 | Index                   | Modifying and computations                            | is_categorical                       | ❌         |             |
|  412 | Index                   | Modifying and computations                            | is_floating                          | ❌         |             |
|  413 | Index                   | Modifying and computations                            | is_integer                           | ❌         |             |
|  414 | Index                   | Modifying and computations                            | is_interval                          | ❌         |             |
|  415 | Index                   | Modifying and computations                            | is_numeric                           | ❌         |             |
|  416 | Index                   | Modifying and computations                            | is_object                            | ❌         |             |
|  417 | Index                   | Modifying and computations                            | max                                  | ❌         |             |
|  418 | Index                   | Modifying and computations                            | min                                  | ❌         |             |
|  419 | Index                   | Modifying and computations                            | nunique                              | ❌         |             |
|  420 | Index                   | Modifying and computations                            | putmask                              | ❌         |             |
|  421 | Index                   | Modifying and computations                            | reindex                              | ❌         |             |
|  422 | Index                   | Modifying and computations                            | rename                               | ❌         |             |
|  423 | Index                   | Modifying and computations                            | repeat                               | ❌         |             |
|  424 | Index                   | Modifying and computations                            | take                                 | ❌         |             |
|  425 | Index                   | Modifying and computations                            | unique                               | ✅         |             |
|  426 | Index                   | Modifying and computations                            | value_counts                         | ✅         |             |
|  427 | Index                   | Modifying and computations                            | where                                | ❌         |             |
|  428 | Index                   | Properties                                            | T                                    | ✅         |             |
|  429 | Index                   | Properties                                            | dtype                                | ✅         |             |
|  430 | Index                   | Properties                                            | empty                                | ✅         |             |
|  431 | Index                   | Properties                                            | has_duplicates                       | ✅         |             |
|  432 | Index                   | Properties                                            | hasnans                              | ❌         |             |
|  433 | Index                   | Properties                                            | inferred_type                        | ❌         |             |
|  434 | Index                   | Properties                                            | is_monotonic_decreasing              | ❌         |             |
|  435 | Index                   | Properties                                            | is_monotonic_increasing              | ❌         |             |
|  436 | Index                   | Properties                                            | is_unique                            | ✅         |             |
|  437 | Index                   | Properties                                            | memory_usage                         | ❌         |             |
|  438 | Index                   | Properties                                            | name                                 | ✅         |             |
|  439 | Index                   | Properties                                            | names                                | ✅         |             |
|  440 | Index                   | Properties                                            | nbytes                               | ❌         |             |
|  441 | Index                   | Properties                                            | ndim                                 | ✅         |             |
|  442 | Index                   | Properties                                            | shape                                | ✅         |             |
|  443 | Index                   | Properties                                            | size                                 | ✅         |             |
|  444 | Index                   | Properties                                            | values                               | ✅         |             |
|  445 | Index                   | Selecting                                             | asof                                 | ❌         |             |
|  446 | Index                   | Selecting                                             | asof_locs                            | ❌         |             |
|  447 | Index                   | Selecting                                             | get_indexer                          | ❌         |             |
|  448 | Index                   | Selecting                                             | get_indexer_for                      | ✅         |             |
|  449 | Index                   | Selecting                                             | get_indexer_non_unique               | ❌         |             |
|  450 | Index                   | Selecting                                             | get_level_values                     | ✅         |             |
|  451 | Index                   | Selecting                                             | get_loc                              | ❌         |             |
|  452 | Index                   | Selecting                                             | get_slice_bound                      | ❌         |             |
|  453 | Index                   | Selecting                                             | isin                                 | ❌         |             |
|  454 | Index                   | Selecting                                             | slice_indexer                        | ✅         |             |
|  455 | Index                   | Selecting                                             | slice_locs                           | ❌         |             |
|  456 | Index                   | Sorting                                               | argsort                              | ❌         |             |
|  457 | Index                   | Sorting                                               | searchsorted                         | ❌         |             |
|  458 | Index                   | Sorting                                               | sort_values                          | ✅         |             |
|  459 | Index                   | Time-specific operations                              | shift                                | ❌         |             |
|  460 | IntervalIndex           | IntervalIndex components                              | closed                               | ❌         |             |
|  461 | IntervalIndex           | IntervalIndex components                              | contains                             | ❌         |             |
|  462 | IntervalIndex           | IntervalIndex components                              | from_arrays                          | ❌         |             |
|  463 | IntervalIndex           | IntervalIndex components                              | from_breaks                          | ✅         |             |
|  464 | IntervalIndex           | IntervalIndex components                              | from_tuples                          | ❌         |             |
|  465 | IntervalIndex           | IntervalIndex components                              | get_indexer                          | ✅         |             |
|  466 | IntervalIndex           | IntervalIndex components                              | get_loc                              | ❌         |             |
|  467 | IntervalIndex           | IntervalIndex components                              | is_empty                             | ❌         |             |
|  468 | IntervalIndex           | IntervalIndex components                              | is_non_overlapping_monotonic         | ❌         |             |
|  469 | IntervalIndex           | IntervalIndex components                              | is_overlapping                       | ✅         |             |
|  470 | IntervalIndex           | IntervalIndex components                              | left                                 | ❌         |             |
|  471 | IntervalIndex           | IntervalIndex components                              | length                               | ❌         |             |
|  472 | IntervalIndex           | IntervalIndex components                              | mid                                  | ❌         |             |
|  473 | IntervalIndex           | IntervalIndex components                              | overlaps                             | ❌         |             |
|  474 | IntervalIndex           | IntervalIndex components                              | right                                | ❌         |             |
|  475 | IntervalIndex           | IntervalIndex components                              | set_closed                           | ❌         |             |
|  476 | IntervalIndex           | IntervalIndex components                              | to_tuples                            | ❌         |             |
|  477 | IntervalIndex           | IntervalIndex components                              | values                               | ❌         |             |
|  478 | MultiIndex              | MultiIndex components                                 | append                               | ✅         |             |
|  479 | MultiIndex              | MultiIndex components                                 | copy                                 | ✅         |             |
|  480 | MultiIndex              | MultiIndex components                                 | drop                                 | ✅         |             |
|  481 | MultiIndex              | MultiIndex components                                 | droplevel                            | ✅         |             |
|  482 | MultiIndex              | MultiIndex components                                 | remove_unused_levels                 | ✅         |             |
|  483 | MultiIndex              | MultiIndex components                                 | reorder_levels                       | ✅         |             |
|  484 | MultiIndex              | MultiIndex components                                 | set_codes                            | ❌         |             |
|  485 | MultiIndex              | MultiIndex components                                 | set_levels                           | ❌         |             |
|  486 | MultiIndex              | MultiIndex components                                 | sortlevel                            | ✅         |             |
|  487 | MultiIndex              | MultiIndex components                                 | swaplevel                            | ❌         |             |
|  488 | MultiIndex              | MultiIndex components                                 | to_flat_index                        | ❌         |             |
|  489 | MultiIndex              | MultiIndex components                                 | to_frame                             | ✅         |             |
|  490 | MultiIndex              | MultiIndex components                                 | truncate                             | ❌         |             |
|  491 | MultiIndex              | MultiIndex constructors                               | from_arrays                          | ✅         |             |
|  492 | MultiIndex              | MultiIndex constructors                               | from_frame                           | ✅         |             |
|  493 | MultiIndex              | MultiIndex constructors                               | from_product                         | ✅         |             |
|  494 | MultiIndex              | MultiIndex constructors                               | from_tuples                          | ✅         |             |
|  495 | MultiIndex              | MultiIndex properties                                 | codes                                | ✅         |             |
|  496 | MultiIndex              | MultiIndex properties                                 | dtypes                               | ❌         |             |
|  497 | MultiIndex              | MultiIndex properties                                 | levels                               | ❌         |             |
|  498 | MultiIndex              | MultiIndex properties                                 | levshape                             | ✅         |             |
|  499 | MultiIndex              | MultiIndex properties                                 | names                                | ✅         |             |
|  500 | MultiIndex              | MultiIndex properties                                 | nlevels                              | ✅         |             |
|  501 | MultiIndex              | MultiIndex selecting                                  | get_indexer                          | ✅         |             |
|  502 | MultiIndex              | MultiIndex selecting                                  | get_level_values                     | ✅         |             |
|  503 | MultiIndex              | MultiIndex selecting                                  | get_loc                              | ✅         |             |
|  504 | MultiIndex              | MultiIndex selecting                                  | get_loc_level                        | ❌         |             |
|  505 | MultiIndex              | MultiIndex selecting                                  | get_locs                             | ✅         |             |
|  506 | PeriodIndex             | Methods                                               | asfreq                               | ❌         |             |
|  507 | PeriodIndex             | Methods                                               | strftime                             | ❌         |             |
|  508 | PeriodIndex             | Methods                                               | to_timestamp                         | ❌         |             |
|  509 | PeriodIndex             | Properties                                            | day                                  | ❌         |             |
|  510 | PeriodIndex             | Properties                                            | day_of_week                          | ❌         |             |
|  511 | PeriodIndex             | Properties                                            | day_of_year                          | ❌         |             |
|  512 | PeriodIndex             | Properties                                            | dayofweek                            | ❌         |             |
|  513 | PeriodIndex             | Properties                                            | dayofyear                            | ❌         |             |
|  514 | PeriodIndex             | Properties                                            | days_in_month                        | ❌         |             |
|  515 | PeriodIndex             | Properties                                            | daysinmonth                          | ❌         |             |
|  516 | PeriodIndex             | Properties                                            | end_time                             | ❌         |             |
|  517 | PeriodIndex             | Properties                                            | freq                                 | ❌         |             |
|  518 | PeriodIndex             | Properties                                            | freqstr                              | ❌         |             |
|  519 | PeriodIndex             | Properties                                            | hour                                 | ❌         |             |
|  520 | PeriodIndex             | Properties                                            | is_leap_year                         | ❌         |             |
|  521 | PeriodIndex             | Properties                                            | minute                               | ❌         |             |
|  522 | PeriodIndex             | Properties                                            | month                                | ❌         |             |
|  523 | PeriodIndex             | Properties                                            | quarter                              | ❌         |             |
|  524 | PeriodIndex             | Properties                                            | qyear                                | ❌         |             |
|  525 | PeriodIndex             | Properties                                            | second                               | ❌         |             |
|  526 | PeriodIndex             | Properties                                            | start_time                           | ❌         |             |
|  527 | PeriodIndex             | Properties                                            | week                                 | ❌         |             |
|  528 | PeriodIndex             | Properties                                            | weekday                              | ❌         |             |
|  529 | PeriodIndex             | Properties                                            | weekofyear                           | ❌         |             |
|  530 | PeriodIndex             | Properties                                            | year                                 | ❌         |             |
|  531 | RangeIndex              | Numeric Index                                         | from_range                           | ❌         |             |
|  532 | RangeIndex              | Numeric Index                                         | start                                | ✅         |             |
|  533 | RangeIndex              | Numeric Index                                         | step                                 | ✅         |             |
|  534 | RangeIndex              | Numeric Index                                         | stop                                 | ✅         |             |
|  535 | Resampler               | Computations / descriptive stats                      | count                                | ✅         |             |
|  536 | Resampler               | Computations / descriptive stats                      | first                                | ✅         |             |
|  537 | Resampler               | Computations / descriptive stats                      | last                                 | ✅         |             |
|  538 | Resampler               | Computations / descriptive stats                      | max                                  | 🟡         | phase 2     |
|  539 | Resampler               | Computations / descriptive stats                      | mean                                 | 🟡         | phase 2     |
|  540 | Resampler               | Computations / descriptive stats                      | median                               | ✅         |             |
|  541 | Resampler               | Computations / descriptive stats                      | min                                  | 🟡         |             |
|  542 | Resampler               | Computations / descriptive stats                      | nunique                              | ❌         |             |
|  543 | Resampler               | Computations / descriptive stats                      | ohlc                                 | ❌         |             |
|  544 | Resampler               | Computations / descriptive stats                      | prod                                 | ❌         |             |
|  545 | Resampler               | Computations / descriptive stats                      | quantile                             | ❌         |             |
|  546 | Resampler               | Computations / descriptive stats                      | sem                                  | ❌         |             |
|  547 | Resampler               | Computations / descriptive stats                      | size                                 | ✅         |             |
|  548 | Resampler               | Computations / descriptive stats                      | std                                  | ✅         | phase 2     |
|  549 | Resampler               | Computations / descriptive stats                      | sum                                  | ✅         | phase 2     |
|  550 | Resampler               | Computations / descriptive stats                      | var                                  | ✅         | phase 2     |
|  551 | Resampler               | Function application                                  | aggregate                            | ❌         |             |
|  552 | Resampler               | Function application                                  | apply                                | ❌         |             |
|  553 | Resampler               | Function application                                  | pipe                                 | ❌         |             |
|  554 | Resampler               | Function application                                  | transform                            | ❌         |             |
|  555 | Resampler               | Indexing, iteration                                   | __iter__                             | ❌         |             |
|  556 | Resampler               | Indexing, iteration                                   | get_group                            | ❌         |             |
|  557 | Resampler               | Indexing, iteration                                   | groups                               | ❌         |             |
|  558 | Resampler               | Indexing, iteration                                   | indices                              | ❌         |             |
|  559 | Resampler               | Upsampling                                            | asfreq                               | ❌         |             |
|  560 | Resampler               | Upsampling                                            | bfill                                | ❌         |             |
|  561 | Resampler               | Upsampling                                            | ffill                                | 🟡         |             |
|  562 | Resampler               | Upsampling                                            | fillna                               | ❌         |             |
|  563 | Resampler               | Upsampling                                            | interpolate                          | ❌         |             |
|  564 | Resampler               | Upsampling                                            | nearest                              | ❌         |             |
|  565 | Rolling                 | Rolling window functions                              | aggregate                            | ❌         |             |
|  566 | Rolling                 | Rolling window functions                              | apply                                | ❌         |             |
|  567 | Rolling                 | Rolling window functions                              | corr                                 | ❌         |             |
|  568 | Rolling                 | Rolling window functions                              | count                                | ✅         |             |
|  569 | Rolling                 | Rolling window functions                              | cov                                  | ❌         |             |
|  570 | Rolling                 | Rolling window functions                              | kurt                                 | ❌         |             |
|  571 | Rolling                 | Rolling window functions                              | max                                  | ✅         | phase 2     |
|  572 | Rolling                 | Rolling window functions                              | mean                                 | ✅         | phase 2     |
|  573 | Rolling                 | Rolling window functions                              | median                               | ❌         |             |
|  574 | Rolling                 | Rolling window functions                              | min                                  | ✅         | phase 2     |
|  575 | Rolling                 | Rolling window functions                              | quantile                             | ❌         |             |
|  576 | Rolling                 | Rolling window functions                              | rank                                 | ❌         |             |
|  577 | Rolling                 | Rolling window functions                              | sem                                  | ✅         |             |
|  578 | Rolling                 | Rolling window functions                              | skew                                 | ❌         |             |
|  579 | Rolling                 | Rolling window functions                              | std                                  | ✅         | phase 2     |
|  580 | Rolling                 | Rolling window functions                              | sum                                  | 🟡         | phase 2     |
|  581 | Rolling                 | Rolling window functions                              | var                                  | ✅         | phase 2     |
|  582 | Series                  | Attributes                                            | T                                    | ✅         | phase 1     |
|  583 | Series                  | Attributes                                            | array                                | ❌         |             |
|  584 | Series                  | Attributes                                            | dtype                                | ✅         |             |
|  585 | Series                  | Attributes                                            | dtypes                               | ✅         |             |
|  586 | Series                  | Attributes                                            | empty                                | ✅         | phase 1     |
|  587 | Series                  | Attributes                                            | flags                                | ❌         |             |
|  588 | Series                  | Attributes                                            | hasnans                              | ❌         |             |
|  589 | Series                  | Attributes                                            | index                                | ✅         | phase 1     |
|  590 | Series                  | Attributes                                            | memory_usage                         | ✅         |             |
|  591 | Series                  | Attributes                                            | name                                 | ✅         |             |
|  592 | Series                  | Attributes                                            | nbytes                               | ❌         |             |
|  593 | Series                  | Attributes                                            | ndim                                 | ✅         |             |
|  594 | Series                  | Attributes                                            | set_flags                            | ❌         |             |
|  595 | Series                  | Attributes                                            | shape                                | ✅         | phase 1     |
|  596 | Series                  | Attributes                                            | size                                 | ✅         | phase 1     |
|  597 | Series                  | Attributes                                            | values                               | ✅         | phase 1     |
|  598 | Series                  | Binary operator functions                             | add                                  | 🟡         | phase 1     |
|  599 | Series                  | Binary operator functions                             | combine                              | ❌         |             |
|  600 | Series                  | Binary operator functions                             | combine_first                        | ❌         |             |
|  601 | Series                  | Binary operator functions                             | div                                  | ✅         |             |
|  602 | Series                  | Binary operator functions                             | dot                                  | ❌         |             |
|  603 | Series                  | Binary operator functions                             | eq                                   | ✅         |             |
|  604 | Series                  | Binary operator functions                             | floordiv                             | ✅         |             |
|  605 | Series                  | Binary operator functions                             | ge                                   | ✅         |             |
|  606 | Series                  | Binary operator functions                             | gt                                   | ✅         |             |
|  607 | Series                  | Binary operator functions                             | le                                   | ✅         |             |
|  608 | Series                  | Binary operator functions                             | lt                                   | ✅         |             |
|  609 | Series                  | Binary operator functions                             | mod                                  | 🟡         |             |
|  610 | Series                  | Binary operator functions                             | mul                                  | ✅         |             |
|  611 | Series                  | Binary operator functions                             | ne                                   | ✅         |             |
|  612 | Series                  | Binary operator functions                             | pow                                  | ✅         |             |
|  613 | Series                  | Binary operator functions                             | product                              | ❌         |             |
|  614 | Series                  | Binary operator functions                             | radd                                 | ✅         |             |
|  615 | Series                  | Binary operator functions                             | rdiv                                 | ✅         |             |
|  616 | Series                  | Binary operator functions                             | rfloordiv                            | ✅         |             |
|  617 | Series                  | Binary operator functions                             | rmod                                 | ✅         |             |
|  618 | Series                  | Binary operator functions                             | rmul                                 | ✅         |             |
|  619 | Series                  | Binary operator functions                             | round                                | ✅         | phase 2     |
|  620 | Series                  | Binary operator functions                             | rpow                                 | ✅         |             |
|  621 | Series                  | Binary operator functions                             | rsub                                 | ✅         |             |
|  622 | Series                  | Binary operator functions                             | rtruediv                             | ✅         |             |
|  623 | Series                  | Binary operator functions                             | sub                                  | ✅         | phase 1     |
|  624 | Series                  | Binary operator functions                             | truediv                              | ✅         |             |
|  625 | Series                  | Categorical accessor                                  | cat.add_categories                   | ❌         |             |
|  626 | Series                  | Categorical accessor                                  | cat.as_ordered                       | ❌         |             |
|  627 | Series                  | Categorical accessor                                  | cat.as_unordered                     | ❌         |             |
|  628 | Series                  | Categorical accessor                                  | cat.categories                       | ❌         |             |
|  629 | Series                  | Categorical accessor                                  | cat.codes                            | ❌         |             |
|  630 | Series                  | Categorical accessor                                  | cat.ordered                          | ❌         |             |
|  631 | Series                  | Categorical accessor                                  | cat.remove_categories                | ❌         |             |
|  632 | Series                  | Categorical accessor                                  | cat.remove_unused_categories         | ❌         |             |
|  633 | Series                  | Categorical accessor                                  | cat.rename_categories                | ❌         |             |
|  634 | Series                  | Categorical accessor                                  | cat.reorder_categories               | ❌         |             |
|  635 | Series                  | Categorical accessor                                  | cat.set_categories                   | ❌         |             |
|  636 | Series                  | Combining / comparing / joining / merging             | compare                              | ❌         |             |
|  637 | Series                  | Combining / comparing / joining / merging             | update                               | ❌         |             |
|  638 | Series                  | Computations / descriptive stats                      | abs                                  | ✅         |             |
|  639 | Series                  | Computations / descriptive stats                      | all                                  | 🟡         |             |
|  640 | Series                  | Computations / descriptive stats                      | any                                  | 🟡         |             |
|  641 | Series                  | Computations / descriptive stats                      | autocorr                             | ❌         |             |
|  642 | Series                  | Computations / descriptive stats                      | between                              | ❌         |             |
|  643 | Series                  | Computations / descriptive stats                      | clip                                 | ❌         |             |
|  644 | Series                  | Computations / descriptive stats                      | corr                                 | ❌         |             |
|  645 | Series                  | Computations / descriptive stats                      | count                                | ✅         | phase 1     |
|  646 | Series                  | Computations / descriptive stats                      | cov                                  | ❌         |             |
|  647 | Series                  | Computations / descriptive stats                      | cummax                               | ✅         | phase 2     |
|  648 | Series                  | Computations / descriptive stats                      | cummin                               | ✅         | phase 2     |
|  649 | Series                  | Computations / descriptive stats                      | cumprod                              | ❌         |             |
|  650 | Series                  | Computations / descriptive stats                      | cumsum                               | ✅         | phase 2     |
|  651 | Series                  | Computations / descriptive stats                      | describe                             | ✅         | phase 2     |
|  652 | Series                  | Computations / descriptive stats                      | diff                                 | ✅         | phase 2     |
|  653 | Series                  | Computations / descriptive stats                      | factorize                            | ❌         |             |
|  654 | Series                  | Computations / descriptive stats                      | is_monotonic_decreasing              | ❌         |             |
|  655 | Series                  | Computations / descriptive stats                      | is_monotonic_increasing              | ❌         |             |
|  656 | Series                  | Computations / descriptive stats                      | is_unique                            | ❌         |             |
|  657 | Series                  | Computations / descriptive stats                      | kurt                                 | ❌         |             |
|  658 | Series                  | Computations / descriptive stats                      | kurtosis                             | ❌         |             |
|  659 | Series                  | Computations / descriptive stats                      | max                                  | ✅         | phase 1     |
|  660 | Series                  | Computations / descriptive stats                      | mean                                 | ✅         | phase 1     |
|  661 | Series                  | Computations / descriptive stats                      | median                               | ✅         | phase 1     |
|  662 | Series                  | Computations / descriptive stats                      | min                                  | ✅         | phase 1     |
|  663 | Series                  | Computations / descriptive stats                      | mode                                 | ❌         |             |
|  664 | Series                  | Computations / descriptive stats                      | nlargest                             | 🟡         |             |
|  665 | Series                  | Computations / descriptive stats                      | nsmallest                            | 🟡         |             |
|  666 | Series                  | Computations / descriptive stats                      | nunique                              | ✅         | phase 1     |
|  667 | Series                  | Computations / descriptive stats                      | pct_change                           | 🟡         |             |
|  668 | Series                  | Computations / descriptive stats                      | prod                                 | ❌         |             |
|  669 | Series                  | Computations / descriptive stats                      | quantile                             | 🟡         | phase 1     |
|  670 | Series                  | Computations / descriptive stats                      | rank                                 | ✅         | phase 2     |
|  671 | Series                  | Computations / descriptive stats                      | sem                                  | ❌         |             |
|  672 | Series                  | Computations / descriptive stats                      | skew                                 | ✅         | phase 2     |
|  673 | Series                  | Computations / descriptive stats                      | std                                  | ✅         | phase 1     |
|  674 | Series                  | Computations / descriptive stats                      | sum                                  | ✅         | phase 1     |
|  675 | Series                  | Computations / descriptive stats                      | unique                               | ✅         | phase 1     |
|  676 | Series                  | Computations / descriptive stats                      | value_counts                         | 🟡         | phase 2     |
|  677 | Series                  | Computations / descriptive stats                      | var                                  | ✅         | phase 1     |
|  678 | Series                  | Conversion                                            | __array__                            | ✅         |             |
|  679 | Series                  | Conversion                                            | astype                               | 🟡         | phase 1     |
|  680 | Series                  | Conversion                                            | bool                                 | ❌         |             |
|  681 | Series                  | Conversion                                            | convert_dtypes                       | ❌         |             |
|  682 | Series                  | Conversion                                            | copy                                 | ✅         | phase 1     |
|  683 | Series                  | Conversion                                            | infer_objects                        | ❌         |             |
|  684 | Series                  | Conversion                                            | to_list                              | ✅         | phase 1     |
|  685 | Series                  | Conversion                                            | to_numpy                             | ✅         | phase 1     |
|  686 | Series                  | Conversion                                            | to_period                            | ❌         |             |
|  687 | Series                  | Conversion                                            | to_timestamp                         | ❌         |             |
|  688 | Series                  | Datetime methods                                      | dt.as_unit                           | ❌         |             |
|  689 | Series                  | Datetime methods                                      | dt.ceil                              | ❌         |             |
|  690 | Series                  | Datetime methods                                      | dt.day_name                          | ❌         |             |
|  691 | Series                  | Datetime methods                                      | dt.floor                             | ❌         |             |
|  692 | Series                  | Datetime methods                                      | dt.isocalendar                       | ✅         |             |
|  693 | Series                  | Datetime methods                                      | dt.month_name                        | ❌         |             |
|  694 | Series                  | Datetime methods                                      | dt.normalize                         | ❌         |             |
|  695 | Series                  | Datetime methods                                      | dt.round                             | ❌         |             |
|  696 | Series                  | Datetime methods                                      | dt.strftime                          | ❌         |             |
|  697 | Series                  | Datetime methods                                      | dt.to_period                         | ❌         |             |
|  698 | Series                  | Datetime methods                                      | dt.to_pydatetime                     | ❌         |             |
|  699 | Series                  | Datetime methods                                      | dt.tz_convert                        | ❌         |             |
|  700 | Series                  | Datetime methods                                      | dt.tz_localize                       | ❌         |             |
|  701 | Series                  | Datetime properties                                   | dt.date                              | ✅         |             |
|  702 | Series                  | Datetime properties                                   | dt.day                               | ✅         |             |
|  703 | Series                  | Datetime properties                                   | dt.day_of_week                       | ✅         |             |
|  704 | Series                  | Datetime properties                                   | dt.day_of_year                       | ✅         |             |
|  705 | Series                  | Datetime properties                                   | dt.dayofweek                         | ✅         |             |
|  706 | Series                  | Datetime properties                                   | dt.dayofyear                         | ✅         |             |
|  707 | Series                  | Datetime properties                                   | dt.days_in_month                     | ❌         |             |
|  708 | Series                  | Datetime properties                                   | dt.daysinmonth                       | ❌         |             |
|  709 | Series                  | Datetime properties                                   | dt.freq                              | ❌         |             |
|  710 | Series                  | Datetime properties                                   | dt.hour                              | ✅         |             |
|  711 | Series                  | Datetime properties                                   | dt.is_leap_year                      | ❌         |             |
|  712 | Series                  | Datetime properties                                   | dt.is_month_end                      | ❌         |             |
|  713 | Series                  | Datetime properties                                   | dt.is_month_start                    | ❌         |             |
|  714 | Series                  | Datetime properties                                   | dt.is_quarter_end                    | ❌         |             |
|  715 | Series                  | Datetime properties                                   | dt.is_quarter_start                  | ❌         |             |
|  716 | Series                  | Datetime properties                                   | dt.is_year_end                       | ❌         |             |
|  717 | Series                  | Datetime properties                                   | dt.is_year_start                     | ❌         |             |
|  718 | Series                  | Datetime properties                                   | dt.microsecond                       | ❌         |             |
|  719 | Series                  | Datetime properties                                   | dt.minute                            | ✅         |             |
|  720 | Series                  | Datetime properties                                   | dt.month                             | ✅         |             |
|  721 | Series                  | Datetime properties                                   | dt.nanosecond                        | ❌         |             |
|  722 | Series                  | Datetime properties                                   | dt.quarter                           | ✅         |             |
|  723 | Series                  | Datetime properties                                   | dt.second                            | ✅         |             |
|  724 | Series                  | Datetime properties                                   | dt.time                              | ❌         |             |
|  725 | Series                  | Datetime properties                                   | dt.timetz                            | ❌         |             |
|  726 | Series                  | Datetime properties                                   | dt.tz                                | ❌         |             |
|  727 | Series                  | Datetime properties                                   | dt.unit                              | ❌         |             |
|  728 | Series                  | Datetime properties                                   | dt.weekday                           | ❌         |             |
|  729 | Series                  | Datetime properties                                   | dt.year                              | ✅         |             |
|  730 | Series                  | Function application, GroupBy & window                | agg                                  | 🟡         | phase 1     |
|  731 | Series                  | Function application, GroupBy & window                | aggregate                            | ✅         |             |
|  732 | Series                  | Function application, GroupBy & window                | apply                                | 🟡         | phase 1     |
|  733 | Series                  | Function application, GroupBy & window                | ewm                                  | ❌         |             |
|  734 | Series                  | Function application, GroupBy & window                | expanding                            | ✅         |             |
|  735 | Series                  | Function application, GroupBy & window                | groupby                              | ✅         | phase 1     |
|  736 | Series                  | Function application, GroupBy & window                | map                                  | 🟡         | on-hold     |
|  737 | Series                  | Function application, GroupBy & window                | pipe                                 | ❌         |             |
|  738 | Series                  | Function application, GroupBy & window                | rolling                              | ✅         | phase 2     |
|  739 | Series                  | Function application, GroupBy & window                | transform                            | ❌         |             |
|  740 | Series                  | Indexing, iteration                                   | __iter__                             | ✅         |             |
|  741 | Series                  | Indexing, iteration                                   | at                                   | ✅         |             |
|  742 | Series                  | Indexing, iteration                                   | get                                  | ❌         | on-hold     |
|  743 | Series                  | Indexing, iteration                                   | iat                                  | ✅         |             |
|  744 | Series                  | Indexing, iteration                                   | iloc                                 | ✅         | phase 1     |
|  745 | Series                  | Indexing, iteration                                   | item                                 | ❌         |             |
|  746 | Series                  | Indexing, iteration                                   | items                                | ❌         | on-hold     |
|  747 | Series                  | Indexing, iteration                                   | keys                                 | ❌         | on-hold     |
|  748 | Series                  | Indexing, iteration                                   | loc                                  | ✅         | phase 1     |
|  749 | Series                  | Indexing, iteration                                   | pop                                  | ❌         |             |
|  750 | Series                  | Indexing, iteration                                   | xs                                   | ❌         |             |
|  751 | Series                  | Metadata                                              | attrs                                | ❌         |             |
|  752 | Series                  | Missing data handling                                 | backfill                             | ❌         |             |
|  753 | Series                  | Missing data handling                                 | bfill                                | ❌         |             |
|  754 | Series                  | Missing data handling                                 | dropna                               | ✅         | phase 1     |
|  755 | Series                  | Missing data handling                                 | ffill                                | ✅         |             |
|  756 | Series                  | Missing data handling                                 | fillna                               | ✅         | phase 1     |
|  757 | Series                  | Missing data handling                                 | interpolate                          | ❌         |             |
|  758 | Series                  | Missing data handling                                 | isna                                 | ✅         | phase 1     |
|  759 | Series                  | Missing data handling                                 | isnull                               | ✅         | phase 1     |
|  760 | Series                  | Missing data handling                                 | notna                                | ✅         | phase 1     |
|  761 | Series                  | Missing data handling                                 | notnull                              | ✅         | phase 1     |
|  762 | Series                  | Missing data handling                                 | pad                                  | ✅         |             |
|  763 | Series                  | Missing data handling                                 | replace                              | 🟡         | phase 2     |
|  764 | Series                  | Period properties                                     | dt.end_time                          | ❌         |             |
|  765 | Series                  | Period properties                                     | dt.qyear                             | ❌         |             |
|  766 | Series                  | Period properties                                     | dt.start_time                        | ❌         |             |
|  767 | Series                  | Plotting                                              | hist                                 | ❌         |             |
|  768 | Series                  | Plotting                                              | plot                                 | ❌         |             |
|  769 | Series                  | Plotting                                              | plot.area                            | ❌         |             |
|  770 | Series                  | Plotting                                              | plot.bar                             | ❌         |             |
|  771 | Series                  | Plotting                                              | plot.barh                            | ❌         |             |
|  772 | Series                  | Plotting                                              | plot.box                             | ❌         |             |
|  773 | Series                  | Plotting                                              | plot.density                         | ❌         |             |
|  774 | Series                  | Plotting                                              | plot.hist                            | ❌         |             |
|  775 | Series                  | Plotting                                              | plot.kde                             | ❌         |             |
|  776 | Series                  | Plotting                                              | plot.line                            | ❌         |             |
|  777 | Series                  | Plotting                                              | plot.pie                             | ❌         |             |
|  778 | Series                  | Plotting and visualization                            | hist                                 | ❌         |             |
|  779 | Series                  | Plotting and visualization                            | plot                                 | ❌         |             |
|  780 | Series                  | Plotting and visualization                            | plot.area                            | ❌         |             |
|  781 | Series                  | Plotting and visualization                            | plot.bar                             | ❌         |             |
|  782 | Series                  | Plotting and visualization                            | plot.barh                            | ❌         |             |
|  783 | Series                  | Plotting and visualization                            | plot.box                             | ❌         |             |
|  784 | Series                  | Plotting and visualization                            | plot.density                         | ❌         |             |
|  785 | Series                  | Plotting and visualization                            | plot.hist                            | ❌         |             |
|  786 | Series                  | Plotting and visualization                            | plot.kde                             | ❌         |             |
|  787 | Series                  | Plotting and visualization                            | plot.line                            | ❌         |             |
|  788 | Series                  | Plotting and visualization                            | plot.pie                             | ❌         |             |
|  789 | Series                  | Reindexing / selection / label manipulation           | add_prefix                           | ✅         | phase 2     |
|  790 | Series                  | Reindexing / selection / label manipulation           | add_suffix                           | ✅         | phase 2     |
|  791 | Series                  | Reindexing / selection / label manipulation           | align                                | ❌         |             |
|  792 | Series                  | Reindexing / selection / label manipulation           | drop                                 | ❌         | phase 1     |
|  793 | Series                  | Reindexing / selection / label manipulation           | drop_duplicates                      | ✅         | phase 2     |
|  794 | Series                  | Reindexing / selection / label manipulation           | droplevel                            | ❌         |             |
|  795 | Series                  | Reindexing / selection / label manipulation           | duplicated                           | ✅         | phase 2     |
|  796 | Series                  | Reindexing / selection / label manipulation           | equals                               | ❌         |             |
|  797 | Series                  | Reindexing / selection / label manipulation           | filter                               | ❌         | on-hold     |
|  798 | Series                  | Reindexing / selection / label manipulation           | first                                | ❌         | on-hold     |
|  799 | Series                  | Reindexing / selection / label manipulation           | head                                 | ✅         | phase 1     |
|  800 | Series                  | Reindexing / selection / label manipulation           | idxmax                               | 🟡         | phase 2     |
|  801 | Series                  | Reindexing / selection / label manipulation           | idxmin                               | 🟡         | phase 2     |
|  802 | Series                  | Reindexing / selection / label manipulation           | isin                                 | ✅         | phase 2     |
|  803 | Series                  | Reindexing / selection / label manipulation           | last                                 | ❌         |             |
|  804 | Series                  | Reindexing / selection / label manipulation           | mask                                 | ✅         |             |
|  805 | Series                  | Reindexing / selection / label manipulation           | reindex                              | ❌         |             |
|  806 | Series                  | Reindexing / selection / label manipulation           | reindex_like                         | ❌         |             |
|  807 | Series                  | Reindexing / selection / label manipulation           | rename                               | 🟡         | phase 1     |
|  808 | Series                  | Reindexing / selection / label manipulation           | rename_axis                          | ❌         |             |
|  809 | Series                  | Reindexing / selection / label manipulation           | reset_index                          | ✅         | phase 1     |
|  810 | Series                  | Reindexing / selection / label manipulation           | sample                               | 🟡         | phase 2     |
|  811 | Series                  | Reindexing / selection / label manipulation           | set_axis                             | ✅         |             |
|  812 | Series                  | Reindexing / selection / label manipulation           | tail                                 | ✅         | phase 1     |
|  813 | Series                  | Reindexing / selection / label manipulation           | take                                 | ✅         |             |
|  814 | Series                  | Reindexing / selection / label manipulation           | truncate                             | ❌         |             |
|  815 | Series                  | Reindexing / selection / label manipulation           | where                                | ✅         | phase 1     |
|  816 | Series                  | Reshaping, sorting                                    | argmax                               | ❌         |             |
|  817 | Series                  | Reshaping, sorting                                    | argmin                               | ❌         |             |
|  818 | Series                  | Reshaping, sorting                                    | argsort                              | ❌         |             |
|  819 | Series                  | Reshaping, sorting                                    | explode                              | ❌         |             |
|  820 | Series                  | Reshaping, sorting                                    | ravel                                | ❌         |             |
|  821 | Series                  | Reshaping, sorting                                    | reorder_levels                       | ❌         |             |
|  822 | Series                  | Reshaping, sorting                                    | repeat                               | ❌         |             |
|  823 | Series                  | Reshaping, sorting                                    | searchsorted                         | ❌         |             |
|  824 | Series                  | Reshaping, sorting                                    | sort_index                           | 🟡         | phase 2     |
|  825 | Series                  | Reshaping, sorting                                    | sort_values                          | 🟡         | phase 1     |
|  826 | Series                  | Reshaping, sorting                                    | squeeze                              | ✅         |             |
|  827 | Series                  | Reshaping, sorting                                    | swaplevel                            | ❌         |             |
|  828 | Series                  | Reshaping, sorting                                    | unstack                              | ❌         |             |
|  829 | Series                  | Reshaping, sorting                                    | view                                 | ❌         |             |
|  830 | Series                  | Serialization / IO / conversion                       | to_clipboard                         | ❌         |             |
|  831 | Series                  | Serialization / IO / conversion                       | to_csv                               | ❌         |             |
|  832 | Series                  | Serialization / IO / conversion                       | to_dict                              | ✅         | phase 2     |
|  833 | Series                  | Serialization / IO / conversion                       | to_excel                             | ❌         |             |
|  834 | Series                  | Serialization / IO / conversion                       | to_frame                             | ✅         |             |
|  835 | Series                  | Serialization / IO / conversion                       | to_hdf                               | ❌         |             |
|  836 | Series                  | Serialization / IO / conversion                       | to_json                              | ❌         |             |
|  837 | Series                  | Serialization / IO / conversion                       | to_latex                             | ❌         |             |
|  838 | Series                  | Serialization / IO / conversion                       | to_markdown                          | ❌         |             |
|  839 | Series                  | Serialization / IO / conversion                       | to_pickle                            | ❌         |             |
|  840 | Series                  | Serialization / IO / conversion                       | to_sql                               | ❌         |             |
|  841 | Series                  | Serialization / IO / conversion                       | to_string                            | ❌         |             |
|  842 | Series                  | Serialization / IO / conversion                       | to_xarray                            | ❌         |             |
|  843 | Series                  | Sparse accessor                                       | sparse.density                       | ❌         |             |
|  844 | Series                  | Sparse accessor                                       | sparse.fill_value                    | ❌         |             |
|  845 | Series                  | Sparse accessor                                       | sparse.from_coo                      | ❌         |             |
|  846 | Series                  | Sparse accessor                                       | sparse.npoints                       | ❌         |             |
|  847 | Series                  | Sparse accessor                                       | sparse.sp_values                     | ❌         |             |
|  848 | Series                  | Sparse accessor                                       | sparse.to_coo                        | ❌         |             |
|  849 | Series                  | String handling                                       | str.capitalize                       | ✅         |             |
|  850 | Series                  | String handling                                       | str.casefold                         | ❌         |             |
|  851 | Series                  | String handling                                       | str.cat                              | ✅         |             |
|  852 | Series                  | String handling                                       | str.center                           | ❌         |             |
|  853 | Series                  | String handling                                       | str.contains                         | ✅         | phase 2     |
|  854 | Series                  | String handling                                       | str.count                            | ✅         | phase 2     |
|  855 | Series                  | String handling                                       | str.decode                           | ❌         |             |
|  856 | Series                  | String handling                                       | str.encode                           | ❌         |             |
|  857 | Series                  | String handling                                       | str.endswith                         | ✅         | phase 2     |
|  858 | Series                  | String handling                                       | str.extract                          | ❌         |             |
|  859 | Series                  | String handling                                       | str.extractall                       | ❌         |             |
|  860 | Series                  | String handling                                       | str.find                             | ❌         |             |
|  861 | Series                  | String handling                                       | str.findall                          | ❌         |             |
|  862 | Series                  | String handling                                       | str.fullmatch                        | ❌         |             |
|  863 | Series                  | String handling                                       | str.get                              | ✅         |             |
|  864 | Series                  | String handling                                       | str.get_dummies                      | ✅         |             |
|  865 | Series                  | String handling                                       | str.index                            | ❌         |             |
|  866 | Series                  | String handling                                       | str.isalnum                          | ❌         |             |
|  867 | Series                  | String handling                                       | str.isalpha                          | ❌         |             |
|  868 | Series                  | String handling                                       | str.isdecimal                        | ❌         |             |
|  869 | Series                  | String handling                                       | str.isdigit                          | ✅         | phase 2     |
|  870 | Series                  | String handling                                       | str.islower                          | ✅         | phase 2     |
|  871 | Series                  | String handling                                       | str.isnumeric                        | ❌         |             |
|  872 | Series                  | String handling                                       | str.isspace                          | ❌         |             |
|  873 | Series                  | String handling                                       | str.istitle                          | ✅         |             |
|  874 | Series                  | String handling                                       | str.isupper                          | ✅         | phase 2     |
|  875 | Series                  | String handling                                       | str.join                             | ❌         |             |
|  876 | Series                  | String handling                                       | str.len                              | ✅         | phase 2     |
|  877 | Series                  | String handling                                       | str.ljust                            | ❌         |             |
|  878 | Series                  | String handling                                       | str.lower                            | ✅         | phase 2     |
|  879 | Series                  | String handling                                       | str.lstrip                           | ✅         |             |
|  880 | Series                  | String handling                                       | str.match                            | ✅         |             |
|  881 | Series                  | String handling                                       | str.normalize                        | ❌         |             |
|  882 | Series                  | String handling                                       | str.pad                              | ❌         |             |
|  883 | Series                  | String handling                                       | str.partition                        | ❌         |             |
|  884 | Series                  | String handling                                       | str.removeprefix                     | ❌         |             |
|  885 | Series                  | String handling                                       | str.removesuffix                     | ❌         |             |
|  886 | Series                  | String handling                                       | str.repeat                           | ❌         |             |
|  887 | Series                  | String handling                                       | str.replace                          | ✅         | phase 2     |
|  888 | Series                  | String handling                                       | str.rfind                            | ✅         |             |
|  889 | Series                  | String handling                                       | str.rindex                           | ❌         |             |
|  890 | Series                  | String handling                                       | str.rjust                            | ❌         |             |
|  891 | Series                  | String handling                                       | str.rpartition                       | ❌         |             |
|  892 | Series                  | String handling                                       | str.rsplit                           | ❌         |             |
|  893 | Series                  | String handling                                       | str.rstrip                           | ✅         |             |
|  894 | Series                  | String handling                                       | str.slice                            | ✅         |             |
|  895 | Series                  | String handling                                       | str.slice_replace                    | ❌         |             |
|  896 | Series                  | String handling                                       | str.split                            | ✅         | phase 2     |
|  897 | Series                  | String handling                                       | str.startswith                       | ✅         | phase 2     |
|  898 | Series                  | String handling                                       | str.strip                            | ✅         | phase 2     |
|  899 | Series                  | String handling                                       | str.swapcase                         | ❌         |             |
|  900 | Series                  | String handling                                       | str.title                            | ✅         |             |
|  901 | Series                  | String handling                                       | str.translate                        | ✅         |             |
|  902 | Series                  | String handling                                       | str.upper                            | ✅         | phase 2     |
|  903 | Series                  | String handling                                       | str.wrap                             | ❌         |             |
|  904 | Series                  | String handling                                       | str.zfill                            | ❌         |             |
|  905 | Series                  | Time Series-related                                   | asfreq                               | ❌         |             |
|  906 | Series                  | Time Series-related                                   | asof                                 | ❌         |             |
|  907 | Series                  | Time Series-related                                   | at_time                              | ❌         |             |
|  908 | Series                  | Time Series-related                                   | between_time                         | ❌         |             |
|  909 | Series                  | Time Series-related                                   | first_valid_index                    | ✅         | phase 2     |
|  910 | Series                  | Time Series-related                                   | last_valid_index                     | ✅         | phase 2     |
|  911 | Series                  | Time Series-related                                   | resample                             | ✅         | phase 1     |
|  912 | Series                  | Time Series-related                                   | shift                                | 🟡         | phase 2     |
|  913 | Series                  | Time Series-related                                   | tz_convert                           | ❌         |             |
|  914 | Series                  | Time Series-related                                   | tz_localize                          | ❌         |             |
|  915 | Series                  | Timedelta methods                                     | dt.as_unit                           | ❌         |             |
|  916 | Series                  | Timedelta methods                                     | dt.to_pytimedelta                    | ❌         |             |
|  917 | Series                  | Timedelta methods                                     | dt.total_seconds                     | ❌         |             |
|  918 | Series                  | Timedelta properties                                  | dt.components                        | ❌         |             |
|  919 | Series                  | Timedelta properties                                  | dt.days                              | ❌         |             |
|  920 | Series                  | Timedelta properties                                  | dt.microseconds                      | ❌         |             |
|  921 | Series                  | Timedelta properties                                  | dt.nanoseconds                       | ❌         |             |
|  922 | Series                  | Timedelta properties                                  | dt.seconds                           | ❌         |             |
|  923 | Series                  | Timedelta properties                                  | dt.unit                              | ❌         |             |
|  924 | SeriesGroupBy           | Function application                                  | agg                                  | ✅         |             |
|  925 | SeriesGroupBy           | Function application                                  | aggregate                            | ❌         |             |
|  926 | SeriesGroupBy           | Function application                                  | apply                                | 🟡         | phase 2     |
|  927 | SeriesGroupBy           | Function application                                  | filter                               | ❌         |             |
|  928 | SeriesGroupBy           | Function application                                  | pipe                                 | ❌         |             |
|  929 | SeriesGroupBy           | Function application                                  | transform                            | 🟡         | phase 2     |
|  930 | SeriesGroupBy           | Indexing, iteration                                   | __iter__                             | ❌         |             |
|  931 | SeriesGroupBy           | Indexing, iteration                                   | get_group                            | ❌         |             |
|  932 | SeriesGroupBy           | Indexing, iteration                                   | groups                               | ✅         |             |
|  933 | SeriesGroupBy           | Indexing, iteration                                   | indices                              | ✅         |             |
|  934 | SeriesGroupBy           | Plotting and visualization                            | hist                                 | ❌         |             |
|  935 | SeriesGroupBy           | Plotting and visualization                            | plot                                 | ❌         |             |
|  936 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | all                                  | ❌         |             |
|  937 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | any                                  | ❌         |             |
|  938 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | bfill                                | ❌         |             |
|  939 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | corr                                 | ❌         |             |
|  940 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | count                                | ✅         |             |
|  941 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cov                                  | ❌         |             |
|  942 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cumcount                             | ✅         | phase 2     |
|  943 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cummax                               | ✅         | phase 2     |
|  944 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cummin                               | ✅         | phase 2     |
|  945 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cumprod                              | ❌         |             |
|  946 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cumsum                               | ✅         | phase 2     |
|  947 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | describe                             | ❌         |             |
|  948 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | diff                                 | ❌         |             |
|  949 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | ffill                                | ❌         |             |
|  950 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | fillna                               | ❌         |             |
|  951 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | first                                | 🟡         |             |
|  952 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | head                                 | ❌         | phase 2     |
|  953 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | idxmax                               | ❌         | phase 2     |
|  954 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | idxmin                               | ❌         | phase 2     |
|  955 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | is_monotonic_decreasing              | ❌         |             |
|  956 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | is_monotonic_increasing              | ❌         |             |
|  957 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | last                                 | 🟡         |             |
|  958 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | max                                  | 🟡         |             |
|  959 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | mean                                 | ✅         |             |
|  960 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | median                               | ✅         |             |
|  961 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | min                                  | ✅         |             |
|  962 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | ngroup                               | ❌         |             |
|  963 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nlargest                             | ❌         |             |
|  964 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nsmallest                            | ❌         |             |
|  965 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nth                                  | ❌         |             |
|  966 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nunique                              | ❌         |             |
|  967 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | ohlc                                 | ❌         |             |
|  968 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | pct_change                           | ❌         |             |
|  969 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | prod                                 | ❌         |             |
|  970 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | quantile                             | ❌         |             |
|  971 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | rank                                 | 🟡         |             |
|  972 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | resample                             | ❌         |             |
|  973 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | rolling                              | ❌         |             |
|  974 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | sample                               | ❌         |             |
|  975 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | sem                                  | ❌         |             |
|  976 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | shift                                | ✅         | phase 2     |
|  977 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | size                                 | 🟡         |             |
|  978 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | skew                                 | ❌         |             |
|  979 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | std                                  | ✅         |             |
|  980 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | sum                                  | ✅         |             |
|  981 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | tail                                 | ❌         | phase 2     |
|  982 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | take                                 | ❌         |             |
|  983 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | unique                               | ❌         |             |
|  984 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | value_counts                         | ❌         |             |
|  985 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | var                                  | ✅         |             |
|  986 | TimedeltaIndex          | Components                                            | components                           | ❌         |             |
|  987 | TimedeltaIndex          | Components                                            | days                                 | ❌         |             |
|  988 | TimedeltaIndex          | Components                                            | inferred_freq                        | ❌         |             |
|  989 | TimedeltaIndex          | Components                                            | microseconds                         | ❌         |             |
|  990 | TimedeltaIndex          | Components                                            | nanoseconds                          | ❌         |             |
|  991 | TimedeltaIndex          | Components                                            | seconds                              | ❌         |             |
|  992 | TimedeltaIndex          | Conversion                                            | as_unit                              | ❌         |             |
|  993 | TimedeltaIndex          | Conversion                                            | ceil                                 | ❌         |             |
|  994 | TimedeltaIndex          | Conversion                                            | floor                                | ❌         |             |
|  995 | TimedeltaIndex          | Conversion                                            | round                                | ❌         |             |
|  996 | TimedeltaIndex          | Conversion                                            | to_frame                             | ❌         |             |
|  997 | TimedeltaIndex          | Conversion                                            | to_pytimedelta                       | ❌         |             |
|  998 | TimedeltaIndex          | Conversion                                            | to_series                            | ❌         |             |
|  999 | TimedeltaIndex          | Methods                                               | mean                                 | ❌         |             |
| 1000 | Window                  | Weighted window functions                             | mean                                 | ❌         |             |
| 1001 | Window                  | Weighted window functions                             | std                                  | ❌         |             |
| 1002 | Window                  | Weighted window functions                             | sum                                  | ❌         |             |
| 1003 | Window                  | Weighted window functions                             | var                                  | ❌         |             |
| 1004 | api                     | Window indexer                                        | indexers.BaseIndexer                 | ❌         |             |
| 1005 | api                     | Window indexer                                        | indexers.FixedForwardWindowIndexer   | ❌         |             |
| 1006 | api                     | Window indexer                                        | indexers.VariableOffsetWindowIndexer | ❌         |             |
| 1007 | pandas                  | Data manipulations                                    | concat                               | ❌         |             |
| 1008 | pandas                  | Data manipulations                                    | crosstab                             | ❌         |             |
| 1009 | pandas                  | Data manipulations                                    | cut                                  | ❌         | phase 2     |
| 1010 | pandas                  | Data manipulations                                    | factorize                            | ❌         |             |
| 1011 | pandas                  | Data manipulations                                    | from_dummies                         | ❌         |             |
| 1012 | pandas                  | Data manipulations                                    | get_dummies                          | ❌         | phase 2     |
| 1013 | pandas                  | Data manipulations                                    | lreshape                             | ❌         |             |
| 1014 | pandas                  | Data manipulations                                    | melt                                 | ❌         | phase 2     |
| 1015 | pandas                  | Data manipulations                                    | merge                                | 🟡         |             |
| 1016 | pandas                  | Data manipulations                                    | merge_asof                           | ❌         |             |
| 1017 | pandas                  | Data manipulations                                    | merge_ordered                        | ❌         |             |
| 1018 | pandas                  | Data manipulations                                    | pivot                                | ❌         |             |
| 1019 | pandas                  | Data manipulations                                    | pivot_table                          | ❌         |             |
| 1020 | pandas                  | Data manipulations                                    | qcut                                 | ❌         | phase 2     |
| 1021 | pandas                  | Data manipulations                                    | unique                               | ❌         |             |
| 1022 | pandas                  | Data manipulations                                    | wide_to_long                         | ❌         |             |
| 1023 | pandas                  | Hashing                                               | util.hash_array                      | ❌         |             |
| 1024 | pandas                  | Hashing                                               | util.hash_pandas_object              | ❌         |             |
| 1025 | pandas                  | Input/Output                                          | ExcelFile                            | ❌         |             |
| 1026 | pandas                  | Input/Output                                          | ExcelFile.book                       | ❌         |             |
| 1027 | pandas                  | Input/Output                                          | ExcelFile.parse                      | ❌         |             |
| 1028 | pandas                  | Input/Output                                          | ExcelFile.sheet_names                | ❌         |             |
| 1029 | pandas                  | Input/Output                                          | ExcelWriter                          | ❌         |             |
| 1030 | pandas                  | Input/Output                                          | HDFStore.append                      | ❌         |             |
| 1031 | pandas                  | Input/Output                                          | HDFStore.get                         | ❌         |             |
| 1032 | pandas                  | Input/Output                                          | HDFStore.groups                      | ❌         |             |
| 1033 | pandas                  | Input/Output                                          | HDFStore.info                        | ❌         |             |
| 1034 | pandas                  | Input/Output                                          | HDFStore.keys                        | ❌         |             |
| 1035 | pandas                  | Input/Output                                          | HDFStore.put                         | ❌         |             |
| 1036 | pandas                  | Input/Output                                          | HDFStore.select                      | ❌         |             |
| 1037 | pandas                  | Input/Output                                          | HDFStore.walk                        | ❌         |             |
| 1038 | pandas                  | Input/Output                                          | io.json.build_table_schema           | ❌         |             |
| 1039 | pandas                  | Input/Output                                          | io.stata.StataReader.data_label      | ❌         |             |
| 1040 | pandas                  | Input/Output                                          | io.stata.StataReader.value_labels    | ❌         |             |
| 1041 | pandas                  | Input/Output                                          | io.stata.StataReader.variable_labels | ❌         |             |
| 1042 | pandas                  | Input/Output                                          | io.stata.StataWriter.write_file      | ❌         |             |
| 1043 | pandas                  | Input/Output                                          | json_normalize                       | ❌         |             |
| 1044 | pandas                  | Input/Output                                          | read_clipboard                       | ❌         |             |
| 1045 | pandas                  | Input/Output                                          | read_csv                             | ❌         |             |
| 1046 | pandas                  | Input/Output                                          | read_excel                           | ❌         |             |
| 1047 | pandas                  | Input/Output                                          | read_feather                         | ❌         |             |
| 1048 | pandas                  | Input/Output                                          | read_fwf                             | ❌         |             |
| 1049 | pandas                  | Input/Output                                          | read_gbq                             | ❌         |             |
| 1050 | pandas                  | Input/Output                                          | read_hdf                             | ❌         |             |
| 1051 | pandas                  | Input/Output                                          | read_html                            | ❌         |             |
| 1052 | pandas                  | Input/Output                                          | read_json                            | ❌         |             |
| 1053 | pandas                  | Input/Output                                          | read_orc                             | ❌         |             |
| 1054 | pandas                  | Input/Output                                          | read_parquet                         | ❌         |             |
| 1055 | pandas                  | Input/Output                                          | read_pickle                          | ❌         |             |
| 1056 | pandas                  | Input/Output                                          | read_sas                             | ❌         |             |
| 1057 | pandas                  | Input/Output                                          | read_spss                            | ❌         |             |
| 1058 | pandas                  | Input/Output                                          | read_sql                             | ❌         |             |
| 1059 | pandas                  | Input/Output                                          | read_sql_query                       | ❌         |             |
| 1060 | pandas                  | Input/Output                                          | read_sql_table                       | ❌         |             |
| 1061 | pandas                  | Input/Output                                          | read_stata                           | ❌         |             |
| 1062 | pandas                  | Input/Output                                          | read_table                           | ❌         |             |
| 1063 | pandas                  | Input/Output                                          | read_xml                             | ❌         |             |
| 1064 | pandas                  | Top-level dealing with Interval data                  | interval_range                       | ❌         |             |
| 1065 | pandas                  | Top-level dealing with datetimelike data              | bdate_range                          | ❌         |             |
| 1066 | pandas                  | Top-level dealing with datetimelike data              | date_range                           | ❌         | phase 2     |
| 1067 | pandas                  | Top-level dealing with datetimelike data              | infer_freq                           | ❌         |             |
| 1068 | pandas                  | Top-level dealing with datetimelike data              | period_range                         | ❌         |             |
| 1069 | pandas                  | Top-level dealing with datetimelike data              | timedelta_range                      | ❌         |             |
| 1070 | pandas                  | Top-level dealing with datetimelike data              | to_datetime                          | ❌         |             |
| 1071 | pandas                  | Top-level dealing with datetimelike data              | to_timedelta                         | ❌         |             |
| 1072 | pandas                  | Top-level dealing with numeric data                   | to_numeric                           | ❌         |             |
| 1073 | pandas                  | Top-level evaluation                                  | eval                                 | ❌         |             |
| 1074 | pandas                  | Top-level missing data                                | isna                                 | ❌         |             |
| 1075 | pandas                  | Top-level missing data                                | isnull                               | ❌         |             |
| 1076 | pandas                  | Top-level missing data                                | notna                                | ❌         |             |
| 1077 | pandas                  | Top-level missing data                                | notnull                              | ❌         |             |