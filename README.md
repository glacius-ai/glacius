![Banner Image](https://i.ibb.co/r4M6N7c/Glacius.png)

## What is Glacius?

A **powerful**, **easy to use**, **resource efficient** feature platform for machine learning. 

### Key Features

- 📈 **Feature Engineering & Transformations**: Glacius can handle projects of any size, from small teams to large enterprises processing petabytes of data.
- 🚀 **Low Latency Feature Serving**: Access features instantly, ensuring predictions are derived from the most recent data.
- 📈 **Feature Registry**: A unified view of all your machine learning features and definitions.
- 🔄 **Feature Versioning**: Keep track of how your features evolve over time.

# Getting Started

####  1. Set Up Your Workspace

Go to glacius.ai and register for an account. After signing in, navigate to "workspaces" and click "create a workspace". It will prompt you to do the following:

- Select an AWS Region
- Enter your desired workspace name
- Create a cross account execution role -  This is the role Glacius will use when processing features
- Create a stack in your AWS account and input the role you created from the step above as the principalARN. 
- After you finish creating your stack, enter the role ARN from the output (this allows Glacius to assume the role you just created)


Congrats! You've set up your first Glacius workspace. 

#### 2. Generate Your API Key

After you've finished setting up your workspace, generate an API key. This is what your client will use to authenticate with our back-end infrastructure.

#### 3. Install Glacius (Pip)


```
pip install glacius
```

And that's it! You're all set to explore the capabilities of Glacius.



## Defining And Registering Features


#### 1. Instantiate a Client

To start, let's instantiate a client. We'll specify the **namespace** "development" as we are simply playing around. We will be using this client to register features and trigger jobs later. Make sure to input your API key here that we generated earlier.

```python filename="main.py"
from glacius.core.data_sources.snowflake import SnowflakeSource
from glacius.core.client import Client

client = Client(api_key="***", namespace="development")
```

#### 2. Define A Data Source

Let's define a data source. We'll use snowflake as an example. This table contains items interaction data on items for our hypothetical e-commerce app. We'll make sure to specify the ***timestamp_col*** as this allows Glacius to perform point-in-time joins.


```python filename="main.py"
from glacius.core.data_sources.snowflake import SnowflakeSource

item_engagement_data_source = SnowflakeSource(
    name = "global_item_engagement_data",
    description = "item engagement data",
    timestamp_col = "timestamp",
    table = "global_item_engagement_data",
    database = "gradiently",
    schema="public" 
)
```

#### 3. Defining Your Feature Bundle

Features are grouped into logical groups called ***feature bundles***. A feature bundle is a logical grouping of features that share an entity and a datasource. For example, we could have a feature bundle for user features, another for item features, and another for user-item features.

Let's define our bundle here and add some aggregation features. we'll also need to specify the **entity** this bundle is attached to. Glacius also supports **composite entities**, but in this example, we have a simple single entity with a single join key.

This defines the following feature (total items clicked) across the different time windows [1,3,5,7] days and also within these categories:
- electronics_accessories
- fashion_apparel
- home_garden


```python filename="main.py"
from glacius.core.feature_bundle import FeatureBundle
from glacius.core.entity import Entity
from glacius.core.dtypes import Int32


user_entity = Entity(keys=["user_id"])

user_bundle = FeatureBundle(
    name="user_feature_bundle",
    description="user features on item engagement data",
    source=item_engagement_data_source,
    entity=user_entity,    
)

categories = ["electronics_accessories", "fashion_apparel", "home_garden"]
time_windows = [1,3,5,7]

for category in categories:  
  user_bundle.add_features([
      Feature(
          name = f"total_items_clicked_{category}_{t}d",
          description = f"total items clicked over {t} days",
          expr = when(col("product_category") == category).then(col("item_click")).otherwise(0),
          dtype = Int32,
          agg=Aggregation(method=AggregationType.SUM, window=timedelta(days=t))
      ) for t time_windows
  ])

```


#### 4. Register Your Feature Bundle

Finally, let's register our bundle. You can now view it from the UI!

```python filename="main.py"
response = client.register(feature_bundles=[user_bundle],
                          commit_msg="Added user feature bundle containing click features\
                          for electronics, fashion, and home garden")
```


## Offline Features

To build offline features for training, we will need an **label datasource** and the list of feature names you're interested in computing. 

The label datasource is the spine of the dataset which includes the events you're interested (for example, click events, or item bought events), and the timestamp of when the event occurred. 
This is crucial so that Glacius can perform a **point-in-time join** to compute what the features were for a given entity at that specific point in time. 

#### 1. Define the label data source.

```python filename="main.py"                                                    
label_datasource = SnowflakeSource(
    name = "user_observation_data",
    description = "user click events table and timestamp",
    timestamp_col = "timestamp",
    table = "user_observation_data",
    database = "gradiently",
    schema="public"
)
```


#### Triggering Offline Job Via the Registry

If you are triggering jobs via the registry, you'll need to specify which namespace version you'd like to use. By default it will use the latest version of the namespace. This ensures backwards compatibility for production pipelines. 

```python filename="main.py"                                                    
job = client.get_offline_features(
    feature_names = [f.name for f in user_bundle.features], 
    labels_datasource=label_datasource, 
    output_path="s3a://my-s3-bucket/offline_features_test_job", 
    namespace_version="latest", 
)
```



#### Triggering Ad Hoc Offline Job

If you'd just like to trigger the job from the feature bundles you've just defined in the notebook, you can pass them in directly as well for ad hoc runs.

```python filename="main.py"                                                    
job = client.get_offline_features(
    feature_bundles=[user_bundle]
    labels_datasource=label_datasource, 
    output_path="s3a://my-s3-bucket/offline_features_test_job", 
)
```

## Online Materialization

Glacius also allows you to materialize features into an ultra low latency online store for real time serving. To materialize, simply list the features you're interested in materializing along with the **namespace version**. 

#### Online Materialization

```python filename="main.py"                                                    
client.materialize_features(feature_names=[f.name for f in user_bundle.features], version="latest")
```


#### Getting Online Features

To get online features, call the get_online_features API with the feature names you're interested in getting, and also the unique IDs associated with the entity you're interested in. 

```python filename="main.py"                                                    
online_features = client.get_online_features(feature_names=[f.name for f in new_bundle.features], entity_ids=[        
     user_entity.id("2139083"),    
     user_entity.id("92098321"),    
     ])
```

If you aren't using python for real time inference, you can also call the API

```
curl --location 'localhost:8000/online-store' \
--header 'x-api-key: ******' \
--header 'Content-Type: application/json' \
--data '{
    "namespace": "development",
    "workspace": "test-dev",
    "feature_names": ["AVG_MUSIC_STREAMING_SECS_1_24H", "AVG_MUSIC_STREAMING_SECS_2_24H", "AVG_MUSIC_STREAMING_SECS_3_24H", "AVG_MUSIC_STREAMING_SECS_4_24H", "AVG_MUSIC_STREAMING_SECS_5_24H", "AVG_MUSIC_STREAMING_SECS_6_24H"],
    "entity_ids": ["USER_ID:1671", "USER_ID:1233", "USER_ID:13821"]
}'
```


