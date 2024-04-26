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
|   52 | DataFrame               | Combining / comparing / joining / merging             | assign                               | ❌         |             |
|   53 | DataFrame               | Combining / comparing / joining / merging             | compare                              | ❌         |             |
|   54 | DataFrame               | Combining / comparing / joining / merging             | join                                 | ✅         | phase 1     |
|   55 | DataFrame               | Combining / comparing / joining / merging             | merge                                | ✅         | phase 1     |
|   56 | DataFrame               | Combining / comparing / joining / merging             | update                               | ❌         |             |
|   57 | DataFrame               | Computations / descriptive stats                      | abs                                  | ✅         |             |
|   58 | DataFrame               | Computations / descriptive stats                      | all                                  | ✅         |             |
|   59 | DataFrame               | Computations / descriptive stats                      | any                                  | ✅         |             |
|   60 | DataFrame               | Computations / descriptive stats                      | clip                                 | ❌         |             |
|   61 | DataFrame               | Computations / descriptive stats                      | corr                                 | ❌         | phase 2     |
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
|   79 | DataFrame               | Computations / descriptive stats                      | nunique                              | ✅         | phase 1     |
|   80 | DataFrame               | Computations / descriptive stats                      | pct_change                           | ❌         |             |
|   81 | DataFrame               | Computations / descriptive stats                      | prod                                 | ❌         |             |
|   82 | DataFrame               | Computations / descriptive stats                      | product                              | ❌         |             |
|   83 | DataFrame               | Computations / descriptive stats                      | quantile                             | 🟡         | phase 1     |
|   84 | DataFrame               | Computations / descriptive stats                      | rank                                 | 🟡         | phase 2     |
|   85 | DataFrame               | Computations / descriptive stats                      | round                                | 🟡         | phase 2     |
|   86 | DataFrame               | Computations / descriptive stats                      | sem                                  | ❌         |             |
|   87 | DataFrame               | Computations / descriptive stats                      | skew                                 | 🟡         | phase 2     |
|   88 | DataFrame               | Computations / descriptive stats                      | std                                  | 🟡         | phase 1     |
|   89 | DataFrame               | Computations / descriptive stats                      | sum                                  | ✅         | phase 1     |
|   90 | DataFrame               | Computations / descriptive stats                      | value_counts                         | ✅         | phase 2     |
|   91 | DataFrame               | Computations / descriptive stats                      | var                                  | 🟡         | phase 1     |
|   92 | DataFrame               | Conversion                                            | astype                               | ✅         | phase 1     |
|   93 | DataFrame               | Conversion                                            | bool                                 | ❌         |             |
|   94 | DataFrame               | Conversion                                            | convert_dtypes                       | ❌         |             |
|   95 | DataFrame               | Conversion                                            | copy                                 | ✅         | phase 1     |
|   96 | DataFrame               | Conversion                                            | infer_objects                        | ❌         |             |
|   97 | DataFrame               | Function application, GroupBy & window                | agg                                  | 🟡         | phase 1     |
|   98 | DataFrame               | Function application, GroupBy & window                | aggregate                            | 🟡         |             |
|   99 | DataFrame               | Function application, GroupBy & window                | apply                                | ✅         | phase 1     |
|  100 | DataFrame               | Function application, GroupBy & window                | applymap                             | ✅         | phase 1     |
|  101 | DataFrame               | Function application, GroupBy & window                | ewm                                  | ❌         |             |
|  102 | DataFrame               | Function application, GroupBy & window                | expanding                            | ❌         |             |
|  103 | DataFrame               | Function application, GroupBy & window                | groupby                              | ✅         | phase 1     |
|  104 | DataFrame               | Function application, GroupBy & window                | pipe                                 | ❌         |             |
|  105 | DataFrame               | Function application, GroupBy & window                | rolling                              | ✅         | phase 2     |
|  106 | DataFrame               | Function application, GroupBy & window                | transform                            | ❌         |             |
|  107 | DataFrame               | Indexing, iteration                                   | __iter__                             | ✅         |             |
|  108 | DataFrame               | Indexing, iteration                                   | at                                   | ❌         |             |
|  109 | DataFrame               | Indexing, iteration                                   | get                                  | ❌         | on-hold     |
|  110 | DataFrame               | Indexing, iteration                                   | head                                 | ✅         | phase 1     |
|  111 | DataFrame               | Indexing, iteration                                   | iat                                  | ❌         |             |
|  112 | DataFrame               | Indexing, iteration                                   | iloc                                 | ✅         | phase 1     |
|  113 | DataFrame               | Indexing, iteration                                   | insert                               | ✅         | phase 1     |
|  114 | DataFrame               | Indexing, iteration                                   | isin                                 | ✅         | phase 2     |
|  115 | DataFrame               | Indexing, iteration                                   | items                                | ❌         | on-hold     |
|  116 | DataFrame               | Indexing, iteration                                   | iterrows                             | ✅         | phase 2     |
|  117 | DataFrame               | Indexing, iteration                                   | itertuples                           | ✅         | phase 2     |
|  118 | DataFrame               | Indexing, iteration                                   | keys                                 | ❌         | on-hold     |
|  119 | DataFrame               | Indexing, iteration                                   | loc                                  | ✅         | phase 1     |
|  120 | DataFrame               | Indexing, iteration                                   | mask                                 | ✅         |             |
|  121 | DataFrame               | Indexing, iteration                                   | pop                                  | ❌         |             |
|  122 | DataFrame               | Indexing, iteration                                   | query                                | ❌         |             |
|  123 | DataFrame               | Indexing, iteration                                   | tail                                 | ✅         | phase 1     |
|  124 | DataFrame               | Indexing, iteration                                   | where                                | ✅         | phase 1     |
|  125 | DataFrame               | Indexing, iteration                                   | xs                                   | ❌         |             |
|  126 | DataFrame               | Metadata                                              | attrs                                | ❌         |             |
|  127 | DataFrame               | Missing data handling                                 | backfill                             | ❌         |             |
|  128 | DataFrame               | Missing data handling                                 | bfill                                | ❌         |             |
|  129 | DataFrame               | Missing data handling                                 | dropna                               | ✅         | phase 1     |
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
|  153 | DataFrame               | Reindexing / selection / label manipulation           | add_prefix                           | ✅         | phase 2     |
|  154 | DataFrame               | Reindexing / selection / label manipulation           | add_suffix                           | ✅         | phase 2     |
|  155 | DataFrame               | Reindexing / selection / label manipulation           | align                                | ❌         |             |
|  156 | DataFrame               | Reindexing / selection / label manipulation           | at_time                              | ❌         |             |
|  157 | DataFrame               | Reindexing / selection / label manipulation           | between_time                         | ❌         |             |
|  158 | DataFrame               | Reindexing / selection / label manipulation           | drop                                 | ✅         | phase 1     |
|  159 | DataFrame               | Reindexing / selection / label manipulation           | drop_duplicates                      | ✅         | phase 2     |
|  160 | DataFrame               | Reindexing / selection / label manipulation           | duplicated                           | ✅         | phase 2     |
|  161 | DataFrame               | Reindexing / selection / label manipulation           | equals                               | ❌         |             |
|  162 | DataFrame               | Reindexing / selection / label manipulation           | filter                               | ❌         | on-hold     |
|  163 | DataFrame               | Reindexing / selection / label manipulation           | first                                | ❌         | on-hold     |
|  164 | DataFrame               | Reindexing / selection / label manipulation           | head                                 | ✅         | phase 1     |
|  165 | DataFrame               | Reindexing / selection / label manipulation           | idxmax                               | 🟡         | phase 2     |
|  166 | DataFrame               | Reindexing / selection / label manipulation           | idxmin                               | 🟡         | phase 2     |
|  167 | DataFrame               | Reindexing / selection / label manipulation           | last                                 | ❌         |             |
|  168 | DataFrame               | Reindexing / selection / label manipulation           | reindex                              | ❌         |             |
|  169 | DataFrame               | Reindexing / selection / label manipulation           | reindex_like                         | ❌         |             |
|  170 | DataFrame               | Reindexing / selection / label manipulation           | rename                               | ✅         | phase 1     |
|  171 | DataFrame               | Reindexing / selection / label manipulation           | rename_axis                          | ✅         |             |
|  172 | DataFrame               | Reindexing / selection / label manipulation           | reset_index                          | ✅         | phase 1     |
|  173 | DataFrame               | Reindexing / selection / label manipulation           | sample                               | ✅         | phase 2     |
|  174 | DataFrame               | Reindexing / selection / label manipulation           | set_axis                             | ✅         |             |
|  175 | DataFrame               | Reindexing / selection / label manipulation           | set_index                            | ✅         | phase 1     |
|  176 | DataFrame               | Reindexing / selection / label manipulation           | tail                                 | ✅         | phase 1     |
|  177 | DataFrame               | Reindexing / selection / label manipulation           | take                                 | ✅         |             |
|  178 | DataFrame               | Reindexing / selection / label manipulation           | truncate                             | ❌         |             |
|  179 | DataFrame               | Reshaping, sorting, transposing                       | T                                    | ✅         | phase 1     |
|  180 | DataFrame               | Reshaping, sorting, transposing                       | droplevel                            | ❌         |             |
|  181 | DataFrame               | Reshaping, sorting, transposing                       | explode                              | ❌         |             |
|  182 | DataFrame               | Reshaping, sorting, transposing                       | melt                                 | 🟡         | phase 2     |
|  183 | DataFrame               | Reshaping, sorting, transposing                       | nlargest                             | ❌         |             |
|  184 | DataFrame               | Reshaping, sorting, transposing                       | nsmallest                            | ❌         |             |
|  185 | DataFrame               | Reshaping, sorting, transposing                       | pivot                                | ❌         |             |
|  186 | DataFrame               | Reshaping, sorting, transposing                       | pivot_table                          | 🟡         | phase 1     |
|  187 | DataFrame               | Reshaping, sorting, transposing                       | reorder_levels                       | ❌         |             |
|  188 | DataFrame               | Reshaping, sorting, transposing                       | sort_index                           | 🟡         | phase 2     |
|  189 | DataFrame               | Reshaping, sorting, transposing                       | sort_values                          | ✅         | phase 1     |
|  190 | DataFrame               | Reshaping, sorting, transposing                       | squeeze                              | ✅         |             |
|  191 | DataFrame               | Reshaping, sorting, transposing                       | stack                                | ❌         |             |
|  192 | DataFrame               | Reshaping, sorting, transposing                       | swapaxes                             | ❌         |             |
|  193 | DataFrame               | Reshaping, sorting, transposing                       | swaplevel                            | ❌         |             |
|  194 | DataFrame               | Reshaping, sorting, transposing                       | to_xarray                            | ❌         |             |
|  195 | DataFrame               | Reshaping, sorting, transposing                       | transpose                            | ✅         | phase 1     |
|  196 | DataFrame               | Reshaping, sorting, transposing                       | unstack                              | ❌         |             |
|  197 | DataFrame               | Serialization / IO / conversion                       | __dataframe__                        | ❌         |             |
|  198 | DataFrame               | Serialization / IO / conversion                       | from_dict                            | ❌         |             |
|  199 | DataFrame               | Serialization / IO / conversion                       | from_records                         | ❌         |             |
|  200 | DataFrame               | Serialization / IO / conversion                       | style                                | ❌         |             |
|  201 | DataFrame               | Serialization / IO / conversion                       | to_clipboard                         | ❌         |             |
|  202 | DataFrame               | Serialization / IO / conversion                       | to_csv                               | ❌         |             |
|  203 | DataFrame               | Serialization / IO / conversion                       | to_dict                              | ✅         | phase 2     |
|  204 | DataFrame               | Serialization / IO / conversion                       | to_excel                             | ❌         |             |
|  205 | DataFrame               | Serialization / IO / conversion                       | to_feather                           | ❌         |             |
|  206 | DataFrame               | Serialization / IO / conversion                       | to_gbq                               | ❌         |             |
|  207 | DataFrame               | Serialization / IO / conversion                       | to_hdf                               | ❌         |             |
|  208 | DataFrame               | Serialization / IO / conversion                       | to_html                              | ❌         |             |
|  209 | DataFrame               | Serialization / IO / conversion                       | to_json                              | ❌         |             |
|  210 | DataFrame               | Serialization / IO / conversion                       | to_latex                             | ❌         |             |
|  211 | DataFrame               | Serialization / IO / conversion                       | to_markdown                          | ❌         |             |
|  212 | DataFrame               | Serialization / IO / conversion                       | to_orc                               | ❌         |             |
|  213 | DataFrame               | Serialization / IO / conversion                       | to_parquet                           | ❌         |             |
|  214 | DataFrame               | Serialization / IO / conversion                       | to_pickle                            | ❌         |             |
|  215 | DataFrame               | Serialization / IO / conversion                       | to_records                           | ❌         |             |
|  216 | DataFrame               | Serialization / IO / conversion                       | to_sql                               | ❌         |             |
|  217 | DataFrame               | Serialization / IO / conversion                       | to_stata                             | ❌         |             |
|  218 | DataFrame               | Serialization / IO / conversion                       | to_string                            | ❌         |             |
|  219 | DataFrame               | Sparse accessor                                       | sparse.density                       | ❌         |             |
|  220 | DataFrame               | Sparse accessor                                       | sparse.from_spmatrix                 | ❌         |             |
|  221 | DataFrame               | Sparse accessor                                       | sparse.to_coo                        | ❌         |             |
|  222 | DataFrame               | Sparse accessor                                       | sparse.to_dense                      | ❌         |             |
|  223 | DataFrame               | Time Series-related                                   | asfreq                               | ❌         |             |
|  224 | DataFrame               | Time Series-related                                   | asof                                 | ❌         |             |
|  225 | DataFrame               | Time Series-related                                   | first_valid_index                    | ✅         | phase 2     |
|  226 | DataFrame               | Time Series-related                                   | last_valid_index                     | ✅         | phase 2     |
|  227 | DataFrame               | Time Series-related                                   | resample                             | ✅         | phase 1     |
|  228 | DataFrame               | Time Series-related                                   | shift                                | 🟡         | phase 2     |
|  229 | DataFrame               | Time Series-related                                   | to_period                            | ❌         |             |
|  230 | DataFrame               | Time Series-related                                   | to_timestamp                         | ❌         |             |
|  231 | DataFrame               | Time Series-related                                   | tz_convert                           | ❌         |             |
|  232 | DataFrame               | Time Series-related                                   | tz_localize                          | ❌         |             |
|  233 | DataFrameGroupBy        | Function application                                  | agg                                  | ✅         |             |
|  234 | DataFrameGroupBy        | Function application                                  | aggregate                            | ✅         |             |
|  235 | DataFrameGroupBy        | Function application                                  | apply                                | 🟡         | phase 2     |
|  236 | DataFrameGroupBy        | Function application                                  | filter                               | ❌         |             |
|  237 | DataFrameGroupBy        | Function application                                  | pipe                                 | ❌         |             |
|  238 | DataFrameGroupBy        | Function application                                  | transform                            | ✅         | phase 2     |
|  239 | DataFrameGroupBy        | Indexing, iteration                                   | __iter__                             | ❌         |             |
|  240 | DataFrameGroupBy        | Indexing, iteration                                   | get_group                            | ❌         |             |
|  241 | DataFrameGroupBy        | Indexing, iteration                                   | groups                               | ✅         |             |
|  242 | DataFrameGroupBy        | Indexing, iteration                                   | indices                              | ✅         |             |
|  243 | DataFrameGroupBy        | Plotting and visualization                            | boxplot                              | ❌         |             |
|  244 | DataFrameGroupBy        | Plotting and visualization                            | hist                                 | ❌         |             |
|  245 | DataFrameGroupBy        | Plotting and visualization                            | plot                                 | ❌         |             |
|  246 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | all                                  | ❌         |             |
|  247 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | any                                  | ❌         |             |
|  248 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | bfill                                | ❌         |             |
|  249 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | corr                                 | ❌         |             |
|  250 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | corrwith                             | ❌         |             |
|  251 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | count                                | ✅         |             |
|  252 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cov                                  | ❌         |             |
|  253 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cumcount                             | 🟡         | phase 2     |
|  254 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cummax                               | ✅         | phase 2     |
|  255 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cummin                               | ✅         | phase 2     |
|  256 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cumprod                              | ❌         |             |
|  257 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | cumsum                               | ✅         | phase 2     |
|  258 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | describe                             | ❌         |             |
|  259 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | diff                                 | ❌         |             |
|  260 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | ffill                                | ❌         |             |
|  261 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | fillna                               | ❌         |             |
|  262 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | first                                | ❌         |             |
|  263 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | head                                 | ✅         | phase 2     |
|  264 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | idxmax                               | 🟡         | phase 2     |
|  265 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | idxmin                               | 🟡         | phase 2     |
|  266 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | last                                 | ❌         |             |
|  267 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | max                                  | 🟡         |             |
|  268 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | mean                                 | ✅         |             |
|  269 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | median                               | ✅         |             |
|  270 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | min                                  | ✅         |             |
|  271 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | ngroup                               | ❌         |             |
|  272 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | nth                                  | ❌         |             |
|  273 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | nunique                              | ✅         |             |
|  274 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | ohlc                                 | ❌         |             |
|  275 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | pct_change                           | ❌         |             |
|  276 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | prod                                 | ❌         |             |
|  277 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | quantile                             | ✅         |             |
|  278 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | rank                                 | 🟡         | phase 2     |
|  279 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | resample                             | ❌         |             |
|  280 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | rolling                              | ❌         |             |
|  281 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | sample                               | ❌         |             |
|  282 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | sem                                  | ❌         |             |
|  283 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | shift                                | 🟡         | phase 2     |
|  284 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | size                                 | ❌         |             |
|  285 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | skew                                 | ❌         |             |
|  286 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | std                                  | ✅         |             |
|  287 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | sum                                  | ✅         |             |
|  288 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | tail                                 | ✅         | phase 2     |
|  289 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | take                                 | ❌         |             |
|  290 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | value_counts                         | ❌         |             |
|  291 | DataFrameGroupBy        | ``DataFrameGroupBy`` computations / descriptive stats | var                                  | ✅         |             |
|  292 | DatetimeIndex           | Conversion                                            | as_unit                              | ❌         |             |
|  293 | DatetimeIndex           | Conversion                                            | to_frame                             | ❌         |             |
|  294 | DatetimeIndex           | Conversion                                            | to_period                            | ❌         |             |
|  295 | DatetimeIndex           | Conversion                                            | to_pydatetime                        | ❌         |             |
|  296 | DatetimeIndex           | Conversion                                            | to_series                            | ❌         |             |
|  297 | DatetimeIndex           | Methods                                               | mean                                 | ❌         |             |
|  298 | DatetimeIndex           | Methods                                               | std                                  | ❌         |             |
|  299 | DatetimeIndex           | Selecting                                             | indexer_at_time                      | ❌         |             |
|  300 | DatetimeIndex           | Selecting                                             | indexer_between_time                 | ❌         |             |
|  301 | DatetimeIndex           | Time-specific operations                              | ceil                                 | ❌         |             |
|  302 | DatetimeIndex           | Time-specific operations                              | day_name                             | ❌         |             |
|  303 | DatetimeIndex           | Time-specific operations                              | floor                                | ❌         |             |
|  304 | DatetimeIndex           | Time-specific operations                              | month_name                           | ❌         |             |
|  305 | DatetimeIndex           | Time-specific operations                              | normalize                            | ❌         |             |
|  306 | DatetimeIndex           | Time-specific operations                              | round                                | ❌         |             |
|  307 | DatetimeIndex           | Time-specific operations                              | snap                                 | ❌         |             |
|  308 | DatetimeIndex           | Time-specific operations                              | strftime                             | ❌         |             |
|  309 | DatetimeIndex           | Time-specific operations                              | tz_convert                           | ✅         |             |
|  310 | DatetimeIndex           | Time-specific operations                              | tz_localize                          | ✅         |             |
|  311 | DatetimeIndex           | Time/date components                                  | date                                 | ✅         |             |
|  312 | DatetimeIndex           | Time/date components                                  | day                                  | ✅         |             |
|  313 | DatetimeIndex           | Time/date components                                  | day_of_week                          | ❌         |             |
|  314 | DatetimeIndex           | Time/date components                                  | day_of_year                          | ❌         |             |
|  315 | DatetimeIndex           | Time/date components                                  | dayofweek                            | ✅         |             |
|  316 | DatetimeIndex           | Time/date components                                  | dayofyear                            | ❌         |             |
|  317 | DatetimeIndex           | Time/date components                                  | freq                                 | ✅         |             |
|  318 | DatetimeIndex           | Time/date components                                  | freqstr                              | ✅         |             |
|  319 | DatetimeIndex           | Time/date components                                  | hour                                 | ✅         |             |
|  320 | DatetimeIndex           | Time/date components                                  | inferred_freq                        | ❌         |             |
|  321 | DatetimeIndex           | Time/date components                                  | is_leap_year                         | ❌         |             |
|  322 | DatetimeIndex           | Time/date components                                  | is_month_end                         | ❌         |             |
|  323 | DatetimeIndex           | Time/date components                                  | is_month_start                       | ❌         |             |
|  324 | DatetimeIndex           | Time/date components                                  | is_quarter_end                       | ❌         |             |
|  325 | DatetimeIndex           | Time/date components                                  | is_quarter_start                     | ❌         |             |
|  326 | DatetimeIndex           | Time/date components                                  | is_year_end                          | ❌         |             |
|  327 | DatetimeIndex           | Time/date components                                  | is_year_start                        | ❌         |             |
|  328 | DatetimeIndex           | Time/date components                                  | microsecond                          | ❌         |             |
|  329 | DatetimeIndex           | Time/date components                                  | minute                               | ✅         |             |
|  330 | DatetimeIndex           | Time/date components                                  | month                                | ✅         |             |
|  331 | DatetimeIndex           | Time/date components                                  | nanosecond                           | ❌         |             |
|  332 | DatetimeIndex           | Time/date components                                  | quarter                              | ✅         |             |
|  333 | DatetimeIndex           | Time/date components                                  | second                               | ✅         |             |
|  334 | DatetimeIndex           | Time/date components                                  | time                                 | ❌         |             |
|  335 | DatetimeIndex           | Time/date components                                  | timetz                               | ❌         |             |
|  336 | DatetimeIndex           | Time/date components                                  | tz                                   | ✅         |             |
|  337 | DatetimeIndex           | Time/date components                                  | weekday                              | ✅         |             |
|  338 | DatetimeIndex           | Time/date components                                  | year                                 | ✅         |             |
|  339 | Expanding               | Expanding window functions                            | aggregate                            | ❌         |             |
|  340 | Expanding               | Expanding window functions                            | apply                                | ❌         |             |
|  341 | Expanding               | Expanding window functions                            | corr                                 | ❌         |             |
|  342 | Expanding               | Expanding window functions                            | count                                | ❌         |             |
|  343 | Expanding               | Expanding window functions                            | cov                                  | ❌         |             |
|  344 | Expanding               | Expanding window functions                            | kurt                                 | ❌         |             |
|  345 | Expanding               | Expanding window functions                            | max                                  | ❌         |             |
|  346 | Expanding               | Expanding window functions                            | mean                                 | ❌         |             |
|  347 | Expanding               | Expanding window functions                            | median                               | ❌         |             |
|  348 | Expanding               | Expanding window functions                            | min                                  | ❌         |             |
|  349 | Expanding               | Expanding window functions                            | quantile                             | ❌         |             |
|  350 | Expanding               | Expanding window functions                            | rank                                 | ❌         |             |
|  351 | Expanding               | Expanding window functions                            | sem                                  | ❌         |             |
|  352 | Expanding               | Expanding window functions                            | skew                                 | ❌         |             |
|  353 | Expanding               | Expanding window functions                            | std                                  | ❌         |             |
|  354 | Expanding               | Expanding window functions                            | sum                                  | ❌         |             |
|  355 | Expanding               | Expanding window functions                            | var                                  | ❌         |             |
|  356 | ExponentialMovingWindow | Exponentially-weighted window functions               | corr                                 | ❌         |             |
|  357 | ExponentialMovingWindow | Exponentially-weighted window functions               | cov                                  | ❌         |             |
|  358 | ExponentialMovingWindow | Exponentially-weighted window functions               | mean                                 | ❌         |             |
|  359 | ExponentialMovingWindow | Exponentially-weighted window functions               | std                                  | ❌         |             |
|  360 | ExponentialMovingWindow | Exponentially-weighted window functions               | sum                                  | ❌         |             |
|  361 | ExponentialMovingWindow | Exponentially-weighted window functions               | var                                  | ❌         |             |
|  362 | Index                   | Combining / joining / set operations                  | append                               | ✅         |             |
|  363 | Index                   | Combining / joining / set operations                  | difference                           | ✅         |             |
|  364 | Index                   | Combining / joining / set operations                  | intersection                         | ✅         |             |
|  365 | Index                   | Combining / joining / set operations                  | join                                 | ✅         |             |
|  366 | Index                   | Combining / joining / set operations                  | symmetric_difference                 | ❌         |             |
|  367 | Index                   | Combining / joining / set operations                  | union                                | ✅         |             |
|  368 | Index                   | Compatibility with MultiIndex                         | droplevel                            | ✅         |             |
|  369 | Index                   | Compatibility with MultiIndex                         | set_names                            | ✅         |             |
|  370 | Index                   | Conversion                                            | astype                               | ✅         |             |
|  371 | Index                   | Conversion                                            | item                                 | ❌         |             |
|  372 | Index                   | Conversion                                            | map                                  | ✅         |             |
|  373 | Index                   | Conversion                                            | ravel                                | ❌         |             |
|  374 | Index                   | Conversion                                            | to_frame                             | ❌         |             |
|  375 | Index                   | Conversion                                            | to_list                              | ✅         |             |
|  376 | Index                   | Conversion                                            | to_series                            | ✅         |             |
|  377 | Index                   | Conversion                                            | view                                 | ✅         |             |
|  378 | Index                   | Missing values                                        | dropna                               | ❌         |             |
|  379 | Index                   | Missing values                                        | fillna                               | ❌         |             |
|  380 | Index                   | Missing values                                        | isna                                 | ✅         |             |
|  381 | Index                   | Missing values                                        | notna                                | ❌         |             |
|  382 | Index                   | Modifying and computations                            | all                                  | ❌         |             |
|  383 | Index                   | Modifying and computations                            | any                                  | ❌         |             |
|  384 | Index                   | Modifying and computations                            | argmax                               | ❌         |             |
|  385 | Index                   | Modifying and computations                            | argmin                               | ❌         |             |
|  386 | Index                   | Modifying and computations                            | copy                                 | ✅         |             |
|  387 | Index                   | Modifying and computations                            | delete                               | ✅         |             |
|  388 | Index                   | Modifying and computations                            | drop                                 | ✅         |             |
|  389 | Index                   | Modifying and computations                            | drop_duplicates                      | ✅         |             |
|  390 | Index                   | Modifying and computations                            | duplicated                           | ✅         |             |
|  391 | Index                   | Modifying and computations                            | equals                               | ✅         |             |
|  392 | Index                   | Modifying and computations                            | factorize                            | ✅         |             |
|  393 | Index                   | Modifying and computations                            | identical                            | ✅         |             |
|  394 | Index                   | Modifying and computations                            | insert                               | ✅         |             |
|  395 | Index                   | Modifying and computations                            | is_                                  | ✅         |             |
|  396 | Index                   | Modifying and computations                            | is_boolean                           | ❌         |             |
|  397 | Index                   | Modifying and computations                            | is_categorical                       | ❌         |             |
|  398 | Index                   | Modifying and computations                            | is_floating                          | ❌         |             |
|  399 | Index                   | Modifying and computations                            | is_integer                           | ❌         |             |
|  400 | Index                   | Modifying and computations                            | is_interval                          | ❌         |             |
|  401 | Index                   | Modifying and computations                            | is_numeric                           | ❌         |             |
|  402 | Index                   | Modifying and computations                            | is_object                            | ❌         |             |
|  403 | Index                   | Modifying and computations                            | max                                  | ✅         |             |
|  404 | Index                   | Modifying and computations                            | min                                  | ✅         |             |
|  405 | Index                   | Modifying and computations                            | nunique                              | ✅         |             |
|  406 | Index                   | Modifying and computations                            | putmask                              | ✅         |             |
|  407 | Index                   | Modifying and computations                            | reindex                              | ✅         |             |
|  408 | Index                   | Modifying and computations                            | rename                               | ✅         |             |
|  409 | Index                   | Modifying and computations                            | repeat                               | ✅         |             |
|  410 | Index                   | Modifying and computations                            | take                                 | ✅         |             |
|  411 | Index                   | Modifying and computations                            | unique                               | ✅         |             |
|  412 | Index                   | Modifying and computations                            | value_counts                         | ✅         |             |
|  413 | Index                   | Modifying and computations                            | where                                | ✅         |             |
|  414 | Index                   | Properties                                            | T                                    | ❌         |             |
|  415 | Index                   | Properties                                            | dtype                                | ❌         |             |
|  416 | Index                   | Properties                                            | empty                                | ❌         |             |
|  417 | Index                   | Properties                                            | has_duplicates                       | ✅         |             |
|  418 | Index                   | Properties                                            | hasnans                              | ❌         |             |
|  419 | Index                   | Properties                                            | inferred_type                        | ❌         |             |
|  420 | Index                   | Properties                                            | is_monotonic_decreasing              | ✅         |             |
|  421 | Index                   | Properties                                            | is_monotonic_increasing              | ✅         |             |
|  422 | Index                   | Properties                                            | is_unique                            | ❌         |             |
|  423 | Index                   | Properties                                            | memory_usage                         | ❌         |             |
|  424 | Index                   | Properties                                            | name                                 | ✅         |             |
|  425 | Index                   | Properties                                            | names                                | ✅         |             |
|  426 | Index                   | Properties                                            | nbytes                               | ❌         |             |
|  427 | Index                   | Properties                                            | ndim                                 | ✅         |             |
|  428 | Index                   | Properties                                            | shape                                | ✅         |             |
|  429 | Index                   | Properties                                            | size                                 | ✅         |             |
|  430 | Index                   | Properties                                            | values                               | ✅         |             |
|  431 | Index                   | Selecting                                             | asof                                 | ❌         |             |
|  432 | Index                   | Selecting                                             | asof_locs                            | ❌         |             |
|  433 | Index                   | Selecting                                             | get_indexer                          | ✅         |             |
|  434 | Index                   | Selecting                                             | get_indexer_for                      | ✅         |             |
|  435 | Index                   | Selecting                                             | get_indexer_non_unique               | ✅         |             |
|  436 | Index                   | Selecting                                             | get_level_values                     | ✅         |             |
|  437 | Index                   | Selecting                                             | get_loc                              | ✅         |             |
|  438 | Index                   | Selecting                                             | get_slice_bound                      | ✅         |             |
|  439 | Index                   | Selecting                                             | isin                                 | ✅         |             |
|  440 | Index                   | Selecting                                             | slice_indexer                        | ✅         |             |
|  441 | Index                   | Selecting                                             | slice_locs                           | ✅         |             |
|  442 | Index                   | Sorting                                               | argsort                              | ✅         |             |
|  443 | Index                   | Sorting                                               | searchsorted                         | ✅         |             |
|  444 | Index                   | Sorting                                               | sort_values                          | ✅         |             |
|  445 | Index                   | Time-specific operations                              | shift                                | ❌         |             |
|  446 | IntervalIndex           | IntervalIndex components                              | closed                               | ❌         |             |
|  447 | IntervalIndex           | IntervalIndex components                              | contains                             | ❌         |             |
|  448 | IntervalIndex           | IntervalIndex components                              | from_arrays                          | ❌         |             |
|  449 | IntervalIndex           | IntervalIndex components                              | from_breaks                          | ✅         |             |
|  450 | IntervalIndex           | IntervalIndex components                              | from_tuples                          | ❌         |             |
|  451 | IntervalIndex           | IntervalIndex components                              | get_indexer                          | ✅         |             |
|  452 | IntervalIndex           | IntervalIndex components                              | get_loc                              | ❌         |             |
|  453 | IntervalIndex           | IntervalIndex components                              | is_empty                             | ❌         |             |
|  454 | IntervalIndex           | IntervalIndex components                              | is_non_overlapping_monotonic         | ❌         |             |
|  455 | IntervalIndex           | IntervalIndex components                              | is_overlapping                       | ✅         |             |
|  456 | IntervalIndex           | IntervalIndex components                              | left                                 | ❌         |             |
|  457 | IntervalIndex           | IntervalIndex components                              | length                               | ❌         |             |
|  458 | IntervalIndex           | IntervalIndex components                              | mid                                  | ❌         |             |
|  459 | IntervalIndex           | IntervalIndex components                              | overlaps                             | ❌         |             |
|  460 | IntervalIndex           | IntervalIndex components                              | right                                | ❌         |             |
|  461 | IntervalIndex           | IntervalIndex components                              | set_closed                           | ❌         |             |
|  462 | IntervalIndex           | IntervalIndex components                              | to_tuples                            | ❌         |             |
|  463 | IntervalIndex           | IntervalIndex components                              | values                               | ❌         |             |
|  464 | MultiIndex              | MultiIndex components                                 | append                               | ✅         |             |
|  465 | MultiIndex              | MultiIndex components                                 | copy                                 | ✅         |             |
|  466 | MultiIndex              | MultiIndex components                                 | drop                                 | ✅         |             |
|  467 | MultiIndex              | MultiIndex components                                 | droplevel                            | ✅         |             |
|  468 | MultiIndex              | MultiIndex components                                 | remove_unused_levels                 | ✅         |             |
|  469 | MultiIndex              | MultiIndex components                                 | reorder_levels                       | ✅         |             |
|  470 | MultiIndex              | MultiIndex components                                 | set_codes                            | ❌         |             |
|  471 | MultiIndex              | MultiIndex components                                 | set_levels                           | ❌         |             |
|  472 | MultiIndex              | MultiIndex components                                 | sortlevel                            | ❌         |             |
|  473 | MultiIndex              | MultiIndex components                                 | swaplevel                            | ❌         |             |
|  474 | MultiIndex              | MultiIndex components                                 | to_flat_index                        | ❌         |             |
|  475 | MultiIndex              | MultiIndex components                                 | to_frame                             | ❌         |             |
|  476 | MultiIndex              | MultiIndex components                                 | truncate                             | ❌         |             |
|  477 | MultiIndex              | MultiIndex constructors                               | from_arrays                          | ✅         |             |
|  478 | MultiIndex              | MultiIndex constructors                               | from_frame                           | ✅         |             |
|  479 | MultiIndex              | MultiIndex constructors                               | from_product                         | ✅         |             |
|  480 | MultiIndex              | MultiIndex constructors                               | from_tuples                          | ✅         |             |
|  481 | MultiIndex              | MultiIndex properties                                 | codes                                | ✅         |             |
|  482 | MultiIndex              | MultiIndex properties                                 | dtypes                               | ❌         |             |
|  483 | MultiIndex              | MultiIndex properties                                 | levels                               | ❌         |             |
|  484 | MultiIndex              | MultiIndex properties                                 | levshape                             | ✅         |             |
|  485 | MultiIndex              | MultiIndex properties                                 | names                                | ✅         |             |
|  486 | MultiIndex              | MultiIndex properties                                 | nlevels                              | ✅         |             |
|  487 | MultiIndex              | MultiIndex selecting                                  | get_indexer                          | ✅         |             |
|  488 | MultiIndex              | MultiIndex selecting                                  | get_level_values                     | ✅         |             |
|  489 | MultiIndex              | MultiIndex selecting                                  | get_loc                              | ✅         |             |
|  490 | MultiIndex              | MultiIndex selecting                                  | get_loc_level                        | ❌         |             |
|  491 | MultiIndex              | MultiIndex selecting                                  | get_locs                             | ✅         |             |
|  492 | PeriodIndex             | Methods                                               | asfreq                               | ❌         |             |
|  493 | PeriodIndex             | Methods                                               | strftime                             | ❌         |             |
|  494 | PeriodIndex             | Methods                                               | to_timestamp                         | ❌         |             |
|  495 | PeriodIndex             | Properties                                            | day                                  | ❌         |             |
|  496 | PeriodIndex             | Properties                                            | day_of_week                          | ❌         |             |
|  497 | PeriodIndex             | Properties                                            | day_of_year                          | ❌         |             |
|  498 | PeriodIndex             | Properties                                            | dayofweek                            | ❌         |             |
|  499 | PeriodIndex             | Properties                                            | dayofyear                            | ❌         |             |
|  500 | PeriodIndex             | Properties                                            | days_in_month                        | ❌         |             |
|  501 | PeriodIndex             | Properties                                            | daysinmonth                          | ❌         |             |
|  502 | PeriodIndex             | Properties                                            | end_time                             | ❌         |             |
|  503 | PeriodIndex             | Properties                                            | freq                                 | ❌         |             |
|  504 | PeriodIndex             | Properties                                            | freqstr                              | ❌         |             |
|  505 | PeriodIndex             | Properties                                            | hour                                 | ❌         |             |
|  506 | PeriodIndex             | Properties                                            | is_leap_year                         | ❌         |             |
|  507 | PeriodIndex             | Properties                                            | minute                               | ❌         |             |
|  508 | PeriodIndex             | Properties                                            | month                                | ❌         |             |
|  509 | PeriodIndex             | Properties                                            | quarter                              | ❌         |             |
|  510 | PeriodIndex             | Properties                                            | qyear                                | ❌         |             |
|  511 | PeriodIndex             | Properties                                            | second                               | ❌         |             |
|  512 | PeriodIndex             | Properties                                            | start_time                           | ❌         |             |
|  513 | PeriodIndex             | Properties                                            | week                                 | ❌         |             |
|  514 | PeriodIndex             | Properties                                            | weekday                              | ❌         |             |
|  515 | PeriodIndex             | Properties                                            | weekofyear                           | ❌         |             |
|  516 | PeriodIndex             | Properties                                            | year                                 | ❌         |             |
|  517 | RangeIndex              | Numeric Index                                         | from_range                           | ❌         |             |
|  518 | RangeIndex              | Numeric Index                                         | start                                | ✅         |             |
|  519 | RangeIndex              | Numeric Index                                         | step                                 | ✅         |             |
|  520 | RangeIndex              | Numeric Index                                         | stop                                 | ✅         |             |
|  521 | Resampler               | Computations / descriptive stats                      | count                                | ❌         |             |
|  522 | Resampler               | Computations / descriptive stats                      | first                                | ❌         |             |
|  523 | Resampler               | Computations / descriptive stats                      | last                                 | ❌         |             |
|  524 | Resampler               | Computations / descriptive stats                      | max                                  | ❌         | phase 2     |
|  525 | Resampler               | Computations / descriptive stats                      | mean                                 | ❌         | phase 2     |
|  526 | Resampler               | Computations / descriptive stats                      | median                               | ❌         |             |
|  527 | Resampler               | Computations / descriptive stats                      | min                                  | ❌         |             |
|  528 | Resampler               | Computations / descriptive stats                      | nunique                              | ❌         |             |
|  529 | Resampler               | Computations / descriptive stats                      | ohlc                                 | ❌         |             |
|  530 | Resampler               | Computations / descriptive stats                      | prod                                 | ❌         |             |
|  531 | Resampler               | Computations / descriptive stats                      | quantile                             | ❌         |             |
|  532 | Resampler               | Computations / descriptive stats                      | sem                                  | ❌         |             |
|  533 | Resampler               | Computations / descriptive stats                      | size                                 | ❌         |             |
|  534 | Resampler               | Computations / descriptive stats                      | std                                  | ❌         | phase 2     |
|  535 | Resampler               | Computations / descriptive stats                      | sum                                  | ❌         | phase 2     |
|  536 | Resampler               | Computations / descriptive stats                      | var                                  | ❌         | phase 2     |
|  537 | Resampler               | Function application                                  | aggregate                            | ❌         |             |
|  538 | Resampler               | Function application                                  | apply                                | ❌         |             |
|  539 | Resampler               | Function application                                  | pipe                                 | ❌         |             |
|  540 | Resampler               | Function application                                  | transform                            | ❌         |             |
|  541 | Resampler               | Indexing, iteration                                   | __iter__                             | ❌         |             |
|  542 | Resampler               | Indexing, iteration                                   | get_group                            | ❌         |             |
|  543 | Resampler               | Indexing, iteration                                   | groups                               | ❌         |             |
|  544 | Resampler               | Indexing, iteration                                   | indices                              | ❌         |             |
|  545 | Resampler               | Upsampling                                            | asfreq                               | ❌         |             |
|  546 | Resampler               | Upsampling                                            | bfill                                | ❌         |             |
|  547 | Resampler               | Upsampling                                            | ffill                                | ❌         |             |
|  548 | Resampler               | Upsampling                                            | fillna                               | ❌         |             |
|  549 | Resampler               | Upsampling                                            | interpolate                          | ❌         |             |
|  550 | Resampler               | Upsampling                                            | nearest                              | ❌         |             |
|  551 | Rolling                 | Rolling window functions                              | aggregate                            | ❌         |             |
|  552 | Rolling                 | Rolling window functions                              | apply                                | ❌         |             |
|  553 | Rolling                 | Rolling window functions                              | corr                                 | ❌         |             |
|  554 | Rolling                 | Rolling window functions                              | count                                | ❌         |             |
|  555 | Rolling                 | Rolling window functions                              | cov                                  | ❌         |             |
|  556 | Rolling                 | Rolling window functions                              | kurt                                 | ❌         |             |
|  557 | Rolling                 | Rolling window functions                              | max                                  | ❌         | phase 2     |
|  558 | Rolling                 | Rolling window functions                              | mean                                 | ❌         | phase 2     |
|  559 | Rolling                 | Rolling window functions                              | median                               | ❌         |             |
|  560 | Rolling                 | Rolling window functions                              | min                                  | ❌         | phase 2     |
|  561 | Rolling                 | Rolling window functions                              | quantile                             | ❌         |             |
|  562 | Rolling                 | Rolling window functions                              | rank                                 | ❌         |             |
|  563 | Rolling                 | Rolling window functions                              | sem                                  | ❌         |             |
|  564 | Rolling                 | Rolling window functions                              | skew                                 | ❌         |             |
|  565 | Rolling                 | Rolling window functions                              | std                                  | ❌         | phase 2     |
|  566 | Rolling                 | Rolling window functions                              | sum                                  | ❌         | phase 2     |
|  567 | Rolling                 | Rolling window functions                              | var                                  | ❌         | phase 2     |
|  568 | Series                  | Attributes                                            | T                                    | ✅         | phase 1     |
|  569 | Series                  | Attributes                                            | array                                | ❌         |             |
|  570 | Series                  | Attributes                                            | dtype                                | ✅         |             |
|  571 | Series                  | Attributes                                            | dtypes                               | ✅         |             |
|  572 | Series                  | Attributes                                            | empty                                | ✅         | phase 1     |
|  573 | Series                  | Attributes                                            | flags                                | ❌         |             |
|  574 | Series                  | Attributes                                            | hasnans                              | ❌         |             |
|  575 | Series                  | Attributes                                            | index                                | ✅         | phase 1     |
|  576 | Series                  | Attributes                                            | memory_usage                         | ✅         |             |
|  577 | Series                  | Attributes                                            | name                                 | ✅         |             |
|  578 | Series                  | Attributes                                            | nbytes                               | ❌         |             |
|  579 | Series                  | Attributes                                            | ndim                                 | ✅         |             |
|  580 | Series                  | Attributes                                            | set_flags                            | ❌         |             |
|  581 | Series                  | Attributes                                            | shape                                | ✅         | phase 1     |
|  582 | Series                  | Attributes                                            | size                                 | ✅         | phase 1     |
|  583 | Series                  | Attributes                                            | values                               | ✅         | phase 1     |
|  584 | Series                  | Binary operator functions                             | add                                  | 🟡         | phase 1     |
|  585 | Series                  | Binary operator functions                             | combine                              | ❌         |             |
|  586 | Series                  | Binary operator functions                             | combine_first                        | ❌         |             |
|  587 | Series                  | Binary operator functions                             | div                                  | ✅         |             |
|  588 | Series                  | Binary operator functions                             | dot                                  | ❌         |             |
|  589 | Series                  | Binary operator functions                             | eq                                   | ✅         |             |
|  590 | Series                  | Binary operator functions                             | floordiv                             | ✅         |             |
|  591 | Series                  | Binary operator functions                             | ge                                   | ✅         |             |
|  592 | Series                  | Binary operator functions                             | gt                                   | ✅         |             |
|  593 | Series                  | Binary operator functions                             | le                                   | ✅         |             |
|  594 | Series                  | Binary operator functions                             | lt                                   | ✅         |             |
|  595 | Series                  | Binary operator functions                             | mod                                  | 🟡         |             |
|  596 | Series                  | Binary operator functions                             | mul                                  | ✅         |             |
|  597 | Series                  | Binary operator functions                             | ne                                   | ✅         |             |
|  598 | Series                  | Binary operator functions                             | pow                                  | ✅         |             |
|  599 | Series                  | Binary operator functions                             | product                              | ❌         |             |
|  600 | Series                  | Binary operator functions                             | radd                                 | ✅         |             |
|  601 | Series                  | Binary operator functions                             | rdiv                                 | ✅         |             |
|  602 | Series                  | Binary operator functions                             | rfloordiv                            | ✅         |             |
|  603 | Series                  | Binary operator functions                             | rmod                                 | ✅         |             |
|  604 | Series                  | Binary operator functions                             | rmul                                 | ✅         |             |
|  605 | Series                  | Binary operator functions                             | round                                | ✅         | phase 2     |
|  606 | Series                  | Binary operator functions                             | rpow                                 | ✅         |             |
|  607 | Series                  | Binary operator functions                             | rsub                                 | ✅         |             |
|  608 | Series                  | Binary operator functions                             | rtruediv                             | ✅         |             |
|  609 | Series                  | Binary operator functions                             | sub                                  | ✅         | phase 1     |
|  610 | Series                  | Binary operator functions                             | truediv                              | ✅         |             |
|  611 | Series                  | Categorical accessor                                  | cat.add_categories                   | ❌         |             |
|  612 | Series                  | Categorical accessor                                  | cat.as_ordered                       | ❌         |             |
|  613 | Series                  | Categorical accessor                                  | cat.as_unordered                     | ❌         |             |
|  614 | Series                  | Categorical accessor                                  | cat.categories                       | ❌         |             |
|  615 | Series                  | Categorical accessor                                  | cat.codes                            | ❌         |             |
|  616 | Series                  | Categorical accessor                                  | cat.ordered                          | ❌         |             |
|  617 | Series                  | Categorical accessor                                  | cat.remove_categories                | ❌         |             |
|  618 | Series                  | Categorical accessor                                  | cat.remove_unused_categories         | ❌         |             |
|  619 | Series                  | Categorical accessor                                  | cat.rename_categories                | ❌         |             |
|  620 | Series                  | Categorical accessor                                  | cat.reorder_categories               | ❌         |             |
|  621 | Series                  | Categorical accessor                                  | cat.set_categories                   | ❌         |             |
|  622 | Series                  | Combining / comparing / joining / merging             | compare                              | ❌         |             |
|  623 | Series                  | Combining / comparing / joining / merging             | update                               | ❌         |             |
|  624 | Series                  | Computations / descriptive stats                      | abs                                  | ✅         |             |
|  625 | Series                  | Computations / descriptive stats                      | all                                  | ✅         |             |
|  626 | Series                  | Computations / descriptive stats                      | any                                  | ✅         |             |
|  627 | Series                  | Computations / descriptive stats                      | autocorr                             | ❌         |             |
|  628 | Series                  | Computations / descriptive stats                      | between                              | ❌         |             |
|  629 | Series                  | Computations / descriptive stats                      | clip                                 | ❌         |             |
|  630 | Series                  | Computations / descriptive stats                      | corr                                 | ❌         |             |
|  631 | Series                  | Computations / descriptive stats                      | count                                | ✅         | phase 1     |
|  632 | Series                  | Computations / descriptive stats                      | cov                                  | ❌         |             |
|  633 | Series                  | Computations / descriptive stats                      | cummax                               | ✅         | phase 2     |
|  634 | Series                  | Computations / descriptive stats                      | cummin                               | ✅         | phase 2     |
|  635 | Series                  | Computations / descriptive stats                      | cumprod                              | ❌         |             |
|  636 | Series                  | Computations / descriptive stats                      | cumsum                               | ✅         | phase 2     |
|  637 | Series                  | Computations / descriptive stats                      | describe                             | ✅         | phase 2     |
|  638 | Series                  | Computations / descriptive stats                      | diff                                 | ✅         | phase 2     |
|  639 | Series                  | Computations / descriptive stats                      | factorize                            | ❌         |             |
|  640 | Series                  | Computations / descriptive stats                      | is_monotonic_decreasing              | ❌         |             |
|  641 | Series                  | Computations / descriptive stats                      | is_monotonic_increasing              | ❌         |             |
|  642 | Series                  | Computations / descriptive stats                      | is_unique                            | ❌         |             |
|  643 | Series                  | Computations / descriptive stats                      | kurt                                 | ❌         |             |
|  644 | Series                  | Computations / descriptive stats                      | kurtosis                             | ❌         |             |
|  645 | Series                  | Computations / descriptive stats                      | max                                  | ✅         | phase 1     |
|  646 | Series                  | Computations / descriptive stats                      | mean                                 | ✅         | phase 1     |
|  647 | Series                  | Computations / descriptive stats                      | median                               | ✅         | phase 1     |
|  648 | Series                  | Computations / descriptive stats                      | min                                  | ✅         | phase 1     |
|  649 | Series                  | Computations / descriptive stats                      | mode                                 | ❌         |             |
|  650 | Series                  | Computations / descriptive stats                      | nlargest                             | ❌         |             |
|  651 | Series                  | Computations / descriptive stats                      | nsmallest                            | ❌         |             |
|  652 | Series                  | Computations / descriptive stats                      | nunique                              | ✅         | phase 1     |
|  653 | Series                  | Computations / descriptive stats                      | pct_change                           | ❌         |             |
|  654 | Series                  | Computations / descriptive stats                      | prod                                 | ❌         |             |
|  655 | Series                  | Computations / descriptive stats                      | quantile                             | 🟡         | phase 1     |
|  656 | Series                  | Computations / descriptive stats                      | rank                                 | ✅         | phase 2     |
|  657 | Series                  | Computations / descriptive stats                      | sem                                  | ❌         |             |
|  658 | Series                  | Computations / descriptive stats                      | skew                                 | ✅         | phase 2     |
|  659 | Series                  | Computations / descriptive stats                      | std                                  | ✅         | phase 1     |
|  660 | Series                  | Computations / descriptive stats                      | sum                                  | ✅         | phase 1     |
|  661 | Series                  | Computations / descriptive stats                      | unique                               | ✅         | phase 1     |
|  662 | Series                  | Computations / descriptive stats                      | value_counts                         | 🟡         | phase 2     |
|  663 | Series                  | Computations / descriptive stats                      | var                                  | ✅         | phase 1     |
|  664 | Series                  | Conversion                                            | __array__                            | ✅         |             |
|  665 | Series                  | Conversion                                            | astype                               | ✅         | phase 1     |
|  666 | Series                  | Conversion                                            | bool                                 | ❌         |             |
|  667 | Series                  | Conversion                                            | convert_dtypes                       | ❌         |             |
|  668 | Series                  | Conversion                                            | copy                                 | ✅         | phase 1     |
|  669 | Series                  | Conversion                                            | infer_objects                        | ❌         |             |
|  670 | Series                  | Conversion                                            | to_list                              | ✅         | phase 1     |
|  671 | Series                  | Conversion                                            | to_numpy                             | ✅         | phase 1     |
|  672 | Series                  | Conversion                                            | to_period                            | ❌         |             |
|  673 | Series                  | Conversion                                            | to_timestamp                         | ❌         |             |
|  674 | Series                  | Datetime methods                                      | dt.as_unit                           | ❌         |             |
|  675 | Series                  | Datetime methods                                      | dt.ceil                              | ❌         |             |
|  676 | Series                  | Datetime methods                                      | dt.day_name                          | ❌         |             |
|  677 | Series                  | Datetime methods                                      | dt.floor                             | ❌         |             |
|  678 | Series                  | Datetime methods                                      | dt.isocalendar                       | ❌         |             |
|  679 | Series                  | Datetime methods                                      | dt.month_name                        | ❌         |             |
|  680 | Series                  | Datetime methods                                      | dt.normalize                         | ❌         |             |
|  681 | Series                  | Datetime methods                                      | dt.round                             | ❌         |             |
|  682 | Series                  | Datetime methods                                      | dt.strftime                          | ❌         |             |
|  683 | Series                  | Datetime methods                                      | dt.to_period                         | ❌         |             |
|  684 | Series                  | Datetime methods                                      | dt.to_pydatetime                     | ❌         |             |
|  685 | Series                  | Datetime methods                                      | dt.tz_convert                        | ❌         |             |
|  686 | Series                  | Datetime methods                                      | dt.tz_localize                       | ❌         |             |
|  687 | Series                  | Datetime properties                                   | dt.date                              | ❌         |             |
|  688 | Series                  | Datetime properties                                   | dt.day                               | ❌         |             |
|  689 | Series                  | Datetime properties                                   | dt.day_of_week                       | ❌         |             |
|  690 | Series                  | Datetime properties                                   | dt.day_of_year                       | ❌         |             |
|  691 | Series                  | Datetime properties                                   | dt.dayofweek                         | ❌         |             |
|  692 | Series                  | Datetime properties                                   | dt.dayofyear                         | ❌         |             |
|  693 | Series                  | Datetime properties                                   | dt.days_in_month                     | ❌         |             |
|  694 | Series                  | Datetime properties                                   | dt.daysinmonth                       | ❌         |             |
|  695 | Series                  | Datetime properties                                   | dt.freq                              | ❌         |             |
|  696 | Series                  | Datetime properties                                   | dt.hour                              | ❌         |             |
|  697 | Series                  | Datetime properties                                   | dt.is_leap_year                      | ❌         |             |
|  698 | Series                  | Datetime properties                                   | dt.is_month_end                      | ❌         |             |
|  699 | Series                  | Datetime properties                                   | dt.is_month_start                    | ❌         |             |
|  700 | Series                  | Datetime properties                                   | dt.is_quarter_end                    | ❌         |             |
|  701 | Series                  | Datetime properties                                   | dt.is_quarter_start                  | ❌         |             |
|  702 | Series                  | Datetime properties                                   | dt.is_year_end                       | ❌         |             |
|  703 | Series                  | Datetime properties                                   | dt.is_year_start                     | ❌         |             |
|  704 | Series                  | Datetime properties                                   | dt.microsecond                       | ❌         |             |
|  705 | Series                  | Datetime properties                                   | dt.minute                            | ❌         |             |
|  706 | Series                  | Datetime properties                                   | dt.month                             | ❌         |             |
|  707 | Series                  | Datetime properties                                   | dt.nanosecond                        | ❌         |             |
|  708 | Series                  | Datetime properties                                   | dt.quarter                           | ❌         |             |
|  709 | Series                  | Datetime properties                                   | dt.second                            | ❌         |             |
|  710 | Series                  | Datetime properties                                   | dt.time                              | ❌         |             |
|  711 | Series                  | Datetime properties                                   | dt.timetz                            | ❌         |             |
|  712 | Series                  | Datetime properties                                   | dt.tz                                | ❌         |             |
|  713 | Series                  | Datetime properties                                   | dt.unit                              | ❌         |             |
|  714 | Series                  | Datetime properties                                   | dt.weekday                           | ❌         |             |
|  715 | Series                  | Datetime properties                                   | dt.year                              | ❌         |             |
|  716 | Series                  | Function application, GroupBy & window                | agg                                  | ❌         | phase 1     |
|  717 | Series                  | Function application, GroupBy & window                | aggregate                            | ✅         |             |
|  718 | Series                  | Function application, GroupBy & window                | apply                                | ✅         | phase 1     |
|  719 | Series                  | Function application, GroupBy & window                | ewm                                  | ❌         |             |
|  720 | Series                  | Function application, GroupBy & window                | expanding                            | ❌         |             |
|  721 | Series                  | Function application, GroupBy & window                | groupby                              | ✅         | phase 1     |
|  722 | Series                  | Function application, GroupBy & window                | map                                  | ✅         | on-hold     |
|  723 | Series                  | Function application, GroupBy & window                | pipe                                 | ❌         |             |
|  724 | Series                  | Function application, GroupBy & window                | rolling                              | ✅         | phase 2     |
|  725 | Series                  | Function application, GroupBy & window                | transform                            | ❌         |             |
|  726 | Series                  | Indexing, iteration                                   | __iter__                             | ✅         |             |
|  727 | Series                  | Indexing, iteration                                   | at                                   | ❌         |             |
|  728 | Series                  | Indexing, iteration                                   | get                                  | ❌         | on-hold     |
|  729 | Series                  | Indexing, iteration                                   | iat                                  | ❌         |             |
|  730 | Series                  | Indexing, iteration                                   | iloc                                 | ✅         | phase 1     |
|  731 | Series                  | Indexing, iteration                                   | item                                 | ❌         |             |
|  732 | Series                  | Indexing, iteration                                   | items                                | ❌         | on-hold     |
|  733 | Series                  | Indexing, iteration                                   | keys                                 | ❌         | on-hold     |
|  734 | Series                  | Indexing, iteration                                   | loc                                  | ✅         | phase 1     |
|  735 | Series                  | Indexing, iteration                                   | pop                                  | ❌         |             |
|  736 | Series                  | Indexing, iteration                                   | xs                                   | ❌         |             |
|  737 | Series                  | Metadata                                              | attrs                                | ❌         |             |
|  738 | Series                  | Missing data handling                                 | backfill                             | ❌         |             |
|  739 | Series                  | Missing data handling                                 | bfill                                | ❌         |             |
|  740 | Series                  | Missing data handling                                 | dropna                               | ✅         | phase 1     |
|  741 | Series                  | Missing data handling                                 | ffill                                | ✅         |             |
|  742 | Series                  | Missing data handling                                 | fillna                               | ✅         | phase 1     |
|  743 | Series                  | Missing data handling                                 | interpolate                          | ❌         |             |
|  744 | Series                  | Missing data handling                                 | isna                                 | ✅         | phase 1     |
|  745 | Series                  | Missing data handling                                 | isnull                               | ✅         | phase 1     |
|  746 | Series                  | Missing data handling                                 | notna                                | ✅         | phase 1     |
|  747 | Series                  | Missing data handling                                 | notnull                              | ✅         | phase 1     |
|  748 | Series                  | Missing data handling                                 | pad                                  | ✅         |             |
|  749 | Series                  | Missing data handling                                 | replace                              | 🟡         | phase 2     |
|  750 | Series                  | Period properties                                     | dt.end_time                          | ❌         |             |
|  751 | Series                  | Period properties                                     | dt.qyear                             | ❌         |             |
|  752 | Series                  | Period properties                                     | dt.start_time                        | ❌         |             |
|  753 | Series                  | Plotting                                              | hist                                 | ❌         |             |
|  754 | Series                  | Plotting                                              | plot                                 | ❌         |             |
|  755 | Series                  | Plotting                                              | plot.area                            | ❌         |             |
|  756 | Series                  | Plotting                                              | plot.bar                             | ❌         |             |
|  757 | Series                  | Plotting                                              | plot.barh                            | ❌         |             |
|  758 | Series                  | Plotting                                              | plot.box                             | ❌         |             |
|  759 | Series                  | Plotting                                              | plot.density                         | ❌         |             |
|  760 | Series                  | Plotting                                              | plot.hist                            | ❌         |             |
|  761 | Series                  | Plotting                                              | plot.kde                             | ❌         |             |
|  762 | Series                  | Plotting                                              | plot.line                            | ❌         |             |
|  763 | Series                  | Plotting                                              | plot.pie                             | ❌         |             |
|  764 | Series                  | Reindexing / selection / label manipulation           | add_prefix                           | ✅         | phase 2     |
|  765 | Series                  | Reindexing / selection / label manipulation           | add_suffix                           | ✅         | phase 2     |
|  766 | Series                  | Reindexing / selection / label manipulation           | align                                | ❌         |             |
|  767 | Series                  | Reindexing / selection / label manipulation           | drop                                 | ❌         | phase 1     |
|  768 | Series                  | Reindexing / selection / label manipulation           | drop_duplicates                      | ✅         | phase 2     |
|  769 | Series                  | Reindexing / selection / label manipulation           | droplevel                            | ❌         |             |
|  770 | Series                  | Reindexing / selection / label manipulation           | duplicated                           | ✅         | phase 2     |
|  771 | Series                  | Reindexing / selection / label manipulation           | equals                               | ❌         |             |
|  772 | Series                  | Reindexing / selection / label manipulation           | filter                               | ❌         | on-hold     |
|  773 | Series                  | Reindexing / selection / label manipulation           | first                                | ❌         | on-hold     |
|  774 | Series                  | Reindexing / selection / label manipulation           | head                                 | ✅         | phase 1     |
|  775 | Series                  | Reindexing / selection / label manipulation           | idxmax                               | 🟡         | phase 2     |
|  776 | Series                  | Reindexing / selection / label manipulation           | idxmin                               | 🟡         | phase 2     |
|  777 | Series                  | Reindexing / selection / label manipulation           | isin                                 | ✅         | phase 2     |
|  778 | Series                  | Reindexing / selection / label manipulation           | last                                 | ❌         |             |
|  779 | Series                  | Reindexing / selection / label manipulation           | mask                                 | ✅         |             |
|  780 | Series                  | Reindexing / selection / label manipulation           | reindex                              | ❌         |             |
|  781 | Series                  | Reindexing / selection / label manipulation           | reindex_like                         | ❌         |             |
|  782 | Series                  | Reindexing / selection / label manipulation           | rename                               | ✅         | phase 1     |
|  783 | Series                  | Reindexing / selection / label manipulation           | rename_axis                          | ❌         |             |
|  784 | Series                  | Reindexing / selection / label manipulation           | reset_index                          | ✅         | phase 1     |
|  785 | Series                  | Reindexing / selection / label manipulation           | sample                               | 🟡         | phase 2     |
|  786 | Series                  | Reindexing / selection / label manipulation           | set_axis                             | ✅         |             |
|  787 | Series                  | Reindexing / selection / label manipulation           | tail                                 | ✅         | phase 1     |
|  788 | Series                  | Reindexing / selection / label manipulation           | take                                 | ✅         |             |
|  789 | Series                  | Reindexing / selection / label manipulation           | truncate                             | ❌         |             |
|  790 | Series                  | Reindexing / selection / label manipulation           | where                                | ✅         | phase 1     |
|  791 | Series                  | Reshaping, sorting                                    | argmax                               | ❌         |             |
|  792 | Series                  | Reshaping, sorting                                    | argmin                               | ❌         |             |
|  793 | Series                  | Reshaping, sorting                                    | argsort                              | ❌         |             |
|  794 | Series                  | Reshaping, sorting                                    | explode                              | ❌         |             |
|  795 | Series                  | Reshaping, sorting                                    | ravel                                | ❌         |             |
|  796 | Series                  | Reshaping, sorting                                    | reorder_levels                       | ❌         |             |
|  797 | Series                  | Reshaping, sorting                                    | repeat                               | ❌         |             |
|  798 | Series                  | Reshaping, sorting                                    | searchsorted                         | ❌         |             |
|  799 | Series                  | Reshaping, sorting                                    | sort_index                           | 🟡         | phase 2     |
|  800 | Series                  | Reshaping, sorting                                    | sort_values                          | ✅         | phase 1     |
|  801 | Series                  | Reshaping, sorting                                    | squeeze                              | ✅         |             |
|  802 | Series                  | Reshaping, sorting                                    | swaplevel                            | ❌         |             |
|  803 | Series                  | Reshaping, sorting                                    | unstack                              | ❌         |             |
|  804 | Series                  | Reshaping, sorting                                    | view                                 | ❌         |             |
|  805 | Series                  | Serialization / IO / conversion                       | to_clipboard                         | ❌         |             |
|  806 | Series                  | Serialization / IO / conversion                       | to_csv                               | ❌         |             |
|  807 | Series                  | Serialization / IO / conversion                       | to_dict                              | ✅         | phase 2     |
|  808 | Series                  | Serialization / IO / conversion                       | to_excel                             | ❌         |             |
|  809 | Series                  | Serialization / IO / conversion                       | to_frame                             | ✅         |             |
|  810 | Series                  | Serialization / IO / conversion                       | to_hdf                               | ❌         |             |
|  811 | Series                  | Serialization / IO / conversion                       | to_json                              | ❌         |             |
|  812 | Series                  | Serialization / IO / conversion                       | to_latex                             | ❌         |             |
|  813 | Series                  | Serialization / IO / conversion                       | to_markdown                          | ❌         |             |
|  814 | Series                  | Serialization / IO / conversion                       | to_pickle                            | ❌         |             |
|  815 | Series                  | Serialization / IO / conversion                       | to_sql                               | ❌         |             |
|  816 | Series                  | Serialization / IO / conversion                       | to_string                            | ❌         |             |
|  817 | Series                  | Serialization / IO / conversion                       | to_xarray                            | ❌         |             |
|  818 | Series                  | Sparse accessor                                       | sparse.density                       | ❌         |             |
|  819 | Series                  | Sparse accessor                                       | sparse.fill_value                    | ❌         |             |
|  820 | Series                  | Sparse accessor                                       | sparse.from_coo                      | ❌         |             |
|  821 | Series                  | Sparse accessor                                       | sparse.npoints                       | ❌         |             |
|  822 | Series                  | Sparse accessor                                       | sparse.sp_values                     | ❌         |             |
|  823 | Series                  | Sparse accessor                                       | sparse.to_coo                        | ❌         |             |
|  824 | Series                  | String handling                                       | str.capitalize                       | ✅         |             |
|  825 | Series                  | String handling                                       | str.casefold                         | ✅         |             |
|  826 | Series                  | String handling                                       | str.cat                              | ✅         |             |
|  827 | Series                  | String handling                                       | str.center                           | ❌         |             |
|  828 | Series                  | String handling                                       | str.contains                         | ✅         | phase 2     |
|  829 | Series                  | String handling                                       | str.count                            | ✅         | phase 2     |
|  830 | Series                  | String handling                                       | str.decode                           | ❌         |             |
|  831 | Series                  | String handling                                       | str.encode                           | ❌         |             |
|  832 | Series                  | String handling                                       | str.endswith                         | ✅         | phase 2     |
|  833 | Series                  | String handling                                       | str.extract                          | ❌         |             |
|  834 | Series                  | String handling                                       | str.extractall                       | ❌         |             |
|  835 | Series                  | String handling                                       | str.find                             | ❌         |             |
|  836 | Series                  | String handling                                       | str.findall                          | ❌         |             |
|  837 | Series                  | String handling                                       | str.fullmatch                        | ❌         |             |
|  838 | Series                  | String handling                                       | str.get                              | ❌         |             |
|  839 | Series                  | String handling                                       | str.get_dummies                      | ✅         |             |
|  840 | Series                  | String handling                                       | str.index                            | ❌         |             |
|  841 | Series                  | String handling                                       | str.isalnum                          | ❌         |             |
|  842 | Series                  | String handling                                       | str.isalpha                          | ❌         |             |
|  843 | Series                  | String handling                                       | str.isdecimal                        | ❌         |             |
|  844 | Series                  | String handling                                       | str.isdigit                          | ✅         | phase 2     |
|  845 | Series                  | String handling                                       | str.islower                          | ✅         | phase 2     |
|  846 | Series                  | String handling                                       | str.isnumeric                        | ❌         |             |
|  847 | Series                  | String handling                                       | str.isspace                          | ❌         |             |
|  848 | Series                  | String handling                                       | str.istitle                          | ✅         |             |
|  849 | Series                  | String handling                                       | str.isupper                          | ✅         | phase 2     |
|  850 | Series                  | String handling                                       | str.join                             | ❌         |             |
|  851 | Series                  | String handling                                       | str.len                              | ✅         | phase 2     |
|  852 | Series                  | String handling                                       | str.ljust                            | ❌         |             |
|  853 | Series                  | String handling                                       | str.lower                            | ✅         | phase 2     |
|  854 | Series                  | String handling                                       | str.lstrip                           | ❌         |             |
|  855 | Series                  | String handling                                       | str.match                            | ❌         |             |
|  856 | Series                  | String handling                                       | str.normalize                        | ❌         |             |
|  857 | Series                  | String handling                                       | str.pad                              | ❌         |             |
|  858 | Series                  | String handling                                       | str.partition                        | ❌         |             |
|  859 | Series                  | String handling                                       | str.removeprefix                     | ❌         |             |
|  860 | Series                  | String handling                                       | str.removesuffix                     | ❌         |             |
|  861 | Series                  | String handling                                       | str.repeat                           | ❌         |             |
|  862 | Series                  | String handling                                       | str.replace                          | ✅         | phase 2     |
|  863 | Series                  | String handling                                       | str.rfind                            | ❌         |             |
|  864 | Series                  | String handling                                       | str.rindex                           | ❌         |             |
|  865 | Series                  | String handling                                       | str.rjust                            | ❌         |             |
|  866 | Series                  | String handling                                       | str.rpartition                       | ❌         |             |
|  867 | Series                  | String handling                                       | str.rsplit                           | ❌         |             |
|  868 | Series                  | String handling                                       | str.rstrip                           | ❌         |             |
|  869 | Series                  | String handling                                       | str.slice                            | ❌         |             |
|  870 | Series                  | String handling                                       | str.slice_replace                    | ❌         |             |
|  871 | Series                  | String handling                                       | str.split                            | ✅         | phase 2     |
|  872 | Series                  | String handling                                       | str.startswith                       | ✅         | phase 2     |
|  873 | Series                  | String handling                                       | str.strip                            | ✅         | phase 2     |
|  874 | Series                  | String handling                                       | str.swapcase                         | ❌         |             |
|  875 | Series                  | String handling                                       | str.title                            | ✅         |             |
|  876 | Series                  | String handling                                       | str.translate                        | ❌         |             |
|  877 | Series                  | String handling                                       | str.upper                            | ✅         | phase 2     |
|  878 | Series                  | String handling                                       | str.wrap                             | ❌         |             |
|  879 | Series                  | String handling                                       | str.zfill                            | ❌         |             |
|  880 | Series                  | Time Series-related                                   | asfreq                               | ❌         |             |
|  881 | Series                  | Time Series-related                                   | asof                                 | ❌         |             |
|  882 | Series                  | Time Series-related                                   | at_time                              | ❌         |             |
|  883 | Series                  | Time Series-related                                   | between_time                         | ❌         |             |
|  884 | Series                  | Time Series-related                                   | first_valid_index                    | ✅         | phase 2     |
|  885 | Series                  | Time Series-related                                   | last_valid_index                     | ✅         | phase 2     |
|  886 | Series                  | Time Series-related                                   | resample                             | ✅         | phase 1     |
|  887 | Series                  | Time Series-related                                   | shift                                | ✅         | phase 2     |
|  888 | Series                  | Time Series-related                                   | tz_convert                           | ❌         |             |
|  889 | Series                  | Time Series-related                                   | tz_localize                          | ❌         |             |
|  890 | Series                  | Timedelta methods                                     | dt.as_unit                           | ❌         |             |
|  891 | Series                  | Timedelta methods                                     | dt.to_pytimedelta                    | ❌         |             |
|  892 | Series                  | Timedelta methods                                     | dt.total_seconds                     | ❌         |             |
|  893 | Series                  | Timedelta properties                                  | dt.components                        | ❌         |             |
|  894 | Series                  | Timedelta properties                                  | dt.days                              | ❌         |             |
|  895 | Series                  | Timedelta properties                                  | dt.microseconds                      | ❌         |             |
|  896 | Series                  | Timedelta properties                                  | dt.nanoseconds                       | ❌         |             |
|  897 | Series                  | Timedelta properties                                  | dt.seconds                           | ❌         |             |
|  898 | Series                  | Timedelta properties                                  | dt.unit                              | ❌         |             |
|  899 | SeriesGroupBy           | Function application                                  | agg                                  | ✅         |             |
|  900 | SeriesGroupBy           | Function application                                  | aggregate                            | ❌         |             |
|  901 | SeriesGroupBy           | Function application                                  | apply                                | ❌         | phase 2     |
|  902 | SeriesGroupBy           | Function application                                  | filter                               | ❌         |             |
|  903 | SeriesGroupBy           | Function application                                  | pipe                                 | ❌         |             |
|  904 | SeriesGroupBy           | Function application                                  | transform                            | ❌         | phase 2     |
|  905 | SeriesGroupBy           | Indexing, iteration                                   | __iter__                             | ❌         |             |
|  906 | SeriesGroupBy           | Indexing, iteration                                   | get_group                            | ❌         |             |
|  907 | SeriesGroupBy           | Indexing, iteration                                   | groups                               | ✅         |             |
|  908 | SeriesGroupBy           | Indexing, iteration                                   | indices                              | ✅         |             |
|  909 | SeriesGroupBy           | Plotting and visualization                            | hist                                 | ❌         |             |
|  910 | SeriesGroupBy           | Plotting and visualization                            | plot                                 | ❌         |             |
|  911 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | all                                  | ❌         |             |
|  912 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | any                                  | ❌         |             |
|  913 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | bfill                                | ❌         |             |
|  914 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | corr                                 | ❌         |             |
|  915 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | count                                | ✅         |             |
|  916 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cov                                  | ❌         |             |
|  917 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cumcount                             | ✅         | phase 2     |
|  918 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cummax                               | ✅         | phase 2     |
|  919 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cummin                               | ✅         | phase 2     |
|  920 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cumprod                              | ❌         |             |
|  921 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | cumsum                               | ✅         | phase 2     |
|  922 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | describe                             | ❌         |             |
|  923 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | diff                                 | ❌         |             |
|  924 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | ffill                                | ❌         |             |
|  925 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | fillna                               | ❌         |             |
|  926 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | first                                | ❌         |             |
|  927 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | head                                 | ❌         | phase 2     |
|  928 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | idxmax                               | ❌         | phase 2     |
|  929 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | idxmin                               | ❌         | phase 2     |
|  930 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | is_monotonic_decreasing              | ❌         |             |
|  931 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | is_monotonic_increasing              | ❌         |             |
|  932 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | last                                 | ❌         |             |
|  933 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | max                                  | 🟡         |             |
|  934 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | mean                                 | ✅         |             |
|  935 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | median                               | ✅         |             |
|  936 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | min                                  | ✅         |             |
|  937 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | ngroup                               | ❌         |             |
|  938 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nlargest                             | ❌         |             |
|  939 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nsmallest                            | ❌         |             |
|  940 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nth                                  | ❌         |             |
|  941 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | nunique                              | ❌         |             |
|  942 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | ohlc                                 | ❌         |             |
|  943 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | pct_change                           | ❌         |             |
|  944 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | prod                                 | ❌         |             |
|  945 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | quantile                             | ❌         |             |
|  946 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | rank                                 | 🟡         |             |
|  947 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | resample                             | ❌         |             |
|  948 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | rolling                              | ❌         |             |
|  949 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | sample                               | ❌         |             |
|  950 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | sem                                  | ❌         |             |
|  951 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | shift                                | ✅         | phase 2     |
|  952 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | size                                 | ❌         |             |
|  953 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | skew                                 | ❌         |             |
|  954 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | std                                  | ✅         |             |
|  955 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | sum                                  | ✅         |             |
|  956 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | tail                                 | ❌         | phase 2     |
|  957 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | take                                 | ❌         |             |
|  958 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | unique                               | ❌         |             |
|  959 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | value_counts                         | ❌         |             |
|  960 | SeriesGroupBy           | ``SeriesGroupBy`` computations / descriptive stats    | var                                  | ✅         |             |
|  961 | TimedeltaIndex          | Components                                            | components                           | ❌         |             |
|  962 | TimedeltaIndex          | Components                                            | days                                 | ❌         |             |
|  963 | TimedeltaIndex          | Components                                            | inferred_freq                        | ❌         |             |
|  964 | TimedeltaIndex          | Components                                            | microseconds                         | ❌         |             |
|  965 | TimedeltaIndex          | Components                                            | nanoseconds                          | ❌         |             |
|  966 | TimedeltaIndex          | Components                                            | seconds                              | ❌         |             |
|  967 | TimedeltaIndex          | Conversion                                            | as_unit                              | ❌         |             |
|  968 | TimedeltaIndex          | Conversion                                            | ceil                                 | ❌         |             |
|  969 | TimedeltaIndex          | Conversion                                            | floor                                | ❌         |             |
|  970 | TimedeltaIndex          | Conversion                                            | round                                | ❌         |             |
|  971 | TimedeltaIndex          | Conversion                                            | to_frame                             | ❌         |             |
|  972 | TimedeltaIndex          | Conversion                                            | to_pytimedelta                       | ❌         |             |
|  973 | TimedeltaIndex          | Conversion                                            | to_series                            | ❌         |             |
|  974 | TimedeltaIndex          | Methods                                               | mean                                 | ❌         |             |
|  975 | Window                  | Weighted window functions                             | mean                                 | ❌         |             |
|  976 | Window                  | Weighted window functions                             | std                                  | ❌         |             |
|  977 | Window                  | Weighted window functions                             | sum                                  | ❌         |             |
|  978 | Window                  | Weighted window functions                             | var                                  | ❌         |             |
|  979 | api                     | Window indexer                                        | indexers.BaseIndexer                 | ❌         |             |
|  980 | api                     | Window indexer                                        | indexers.FixedForwardWindowIndexer   | ❌         |             |
|  981 | api                     | Window indexer                                        | indexers.VariableOffsetWindowIndexer | ❌         |             |
|  982 | pandas                  | Data manipulations                                    | concat                               | 🟡         |             |
|  983 | pandas                  | Data manipulations                                    | crosstab                             | ❌         |             |
|  984 | pandas                  | Data manipulations                                    | cut                                  | 🟡         | phase 2     |
|  985 | pandas                  | Data manipulations                                    | factorize                            | ❌         |             |
|  986 | pandas                  | Data manipulations                                    | from_dummies                         | ❌         |             |
|  987 | pandas                  | Data manipulations                                    | get_dummies                          | 🟡         | phase 2     |
|  988 | pandas                  | Data manipulations                                    | lreshape                             | ❌         |             |
|  989 | pandas                  | Data manipulations                                    | melt                                 | ✅         | phase 2     |
|  990 | pandas                  | Data manipulations                                    | merge                                | ✅         |             |
|  991 | pandas                  | Data manipulations                                    | merge_asof                           | ❌         |             |
|  992 | pandas                  | Data manipulations                                    | merge_ordered                        | ❌         |             |
|  993 | pandas                  | Data manipulations                                    | pivot                                | ❌         |             |
|  994 | pandas                  | Data manipulations                                    | pivot_table                          | ❌         |             |
|  995 | pandas                  | Data manipulations                                    | qcut                                 | ✅         | phase 2     |
|  996 | pandas                  | Data manipulations                                    | unique                               | ✅         |             |
|  997 | pandas                  | Data manipulations                                    | wide_to_long                         | ❌         |             |
|  998 | pandas                  | Hashing                                               | util.hash_array                      | ❌         |             |
|  999 | pandas                  | Hashing                                               | util.hash_pandas_object              | ❌         |             |
| 1000 | pandas                  | Top-level dealing with Interval data                  | interval_range                       | ❌         |             |
| 1001 | pandas                  | Top-level dealing with datetimelike data              | bdate_range                          | ✅         |             |
| 1002 | pandas                  | Top-level dealing with datetimelike data              | date_range                           | 🟡         | phase 2     |
| 1003 | pandas                  | Top-level dealing with datetimelike data              | infer_freq                           | ❌         |             |
| 1004 | pandas                  | Top-level dealing with datetimelike data              | period_range                         | ✅         |             |
| 1005 | pandas                  | Top-level dealing with datetimelike data              | timedelta_range                      | ✅         |             |
| 1006 | pandas                  | Top-level dealing with datetimelike data              | to_datetime                          | ✅         |             |
| 1007 | pandas                  | Top-level dealing with datetimelike data              | to_timedelta                         | ❌         |             |
| 1008 | pandas                  | Top-level dealing with numeric data                   | to_numeric                           | ✅         |             |
| 1009 | pandas                  | Top-level evaluation                                  | eval                                 | ❌         |             |
| 1010 | pandas                  | Top-level missing data                                | isna                                 | ✅         |             |
| 1011 | pandas                  | Top-level missing data                                | isnull                               | ❌         |             |
| 1012 | pandas                  | Top-level missing data                                | notna                                | ❌         |             |
| 1013 | pandas                  | Top-level missing data                                | notnull                              | ❌         |             |