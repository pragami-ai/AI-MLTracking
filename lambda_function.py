import boto3
import json
import os
import uuid
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr


ce = boto3.client("ce", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

# Env variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "bedrock_usage")
SOCIAL_LENS_TABLE = os.environ.get("SOCIAL_LENS_TABLE", "Social_Lens")

# Static API key
API_KEY = os.environ.get("STATIC_API_KEY")


def get_service_costs(start, end, services):
    try:
        response1 = ce.get_cost_and_usage(
            TimePeriod={"Start": start, "End": end},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            Filter={"Dimensions": {"Key": "SERVICE", "Values": services}},
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "DIMENSION", "Key": "USAGE_TYPE"},
            ],
        )

        response2 = ce.get_cost_and_usage(
            TimePeriod={"Start": start, "End": end},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            Filter={"Dimensions": {"Key": "SERVICE", "Values": services}},
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "DIMENSION", "Key": "REGION"},
            ],
        )
    except Exception as e:
        raise Exception(f"Error fetching cost data: {e}")

    daily_costs = defaultdict(lambda: defaultdict(list))
    usage_type_data = defaultdict(lambda: defaultdict(dict))
    region_data = defaultdict(lambda: defaultdict(dict))

    for result in response1["ResultsByTime"]:
        date = result["TimePeriod"]["Start"]
        for group in result["Groups"]:
            service, usage_type = group["Keys"]
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            if amount > 0:
                usage_type_data[date][service][usage_type] = amount

    for result in response2["ResultsByTime"]:
        date = result["TimePeriod"]["Start"]
        for group in result["Groups"]:
            service, region = group["Keys"]
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            if amount > 0:
                region_data[date][service][region] = amount

    for date in usage_type_data:
        for service in usage_type_data[date]:
            for usage_type, amount in usage_type_data[date][service].items():
                region = "unknown"
                if date in region_data and service in region_data[date]:
                    if len(region_data[date][service]) == 1:
                        region = list(region_data[date][service].keys())[0]
                    else:
                        region = max(
                            region_data[date][service],
                            key=lambda r: region_data[date][service][r],
                        )

                daily_costs[date][service].append(
                    {"usage_type": usage_type, "region": region, "amount": amount}
                )

    return daily_costs


def get_perplexity_costs(start_date, end_date):
    """Fetch Perplexity API costs from Social_Lens DynamoDB table"""
    try:
        print(f"DEBUG: Starting Perplexity cost fetch for date range: {start_date} to {end_date}")
        
        # Connect to the Social_Lens table in ap-south-1 region
        social_lens_dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
        table = social_lens_dynamodb.Table(SOCIAL_LENS_TABLE)
        
        print(f"DEBUG: Connected to table: {SOCIAL_LENS_TABLE} in ap-south-1")
        
        # Convert dates to datetime for comparison
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)  # Include end date
        
        print(f"DEBUG: Date range converted - Start: {start_datetime}, End: {end_datetime}")
        
        # Scan the table to get all items (you might want to optimize this with a GSI if table is large)
        response = table.scan()
        items = response['Items']
        
        print(f"DEBUG: Initial scan returned {len(items)} items")
        
        # Handle pagination if there are more items
        total_scanned = len(items)
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
            total_scanned += len(response['Items'])
            print(f"DEBUG: Pagination - Total items scanned so far: {total_scanned}")
        
        print(f"DEBUG: Total items scanned: {len(items)}")
        
        # Debug: Show a sample of items to understand the structure
        if items:
            print(f"DEBUG: Sample item structure:")
            sample_item = items[0]
            print(f"DEBUG: Sample keys: {list(sample_item.keys())}")
            if 'completed_at' in sample_item:
                print(f"DEBUG: Sample completed_at: {sample_item['completed_at']}")
            if 'ml_cost' in sample_item:
                print(f"DEBUG: Sample ml_cost: {sample_item['ml_cost']} (type: {type(sample_item['ml_cost'])})")
        
        # Process the data by date
        daily_perplexity_costs = defaultdict(list)
        processed_count = 0
        matched_count = 0
        error_count = 0
        
        for item in items:
            processed_count += 1
            try:
                # Extract completed_at and ml_cost
                completed_at = item.get('completed_at')
                ml_cost = item.get('ml_cost')
                
                if not completed_at or ml_cost is None:
                    continue
                
                # Parse the completed_at timestamp (assuming ISO format)
                if isinstance(completed_at, str):
                    # Handle different possible timestamp formats
                    try:
                        if 'T' in completed_at:
                            # Handle ISO format with timezone
                            completed_at_clean = completed_at.replace('Z', '+00:00')
                            if '+' not in completed_at_clean and completed_at_clean.endswith('00:00'):
                                pass  # Already has timezone
                            elif '+' not in completed_at_clean and 'Z' not in completed_at:
                                completed_at_clean = completed_at + '+00:00'
                            item_datetime = datetime.fromisoformat(completed_at_clean)
                        else:
                            item_datetime = datetime.strptime(completed_at, "%Y-%m-%d")
                    except Exception as parse_error:
                        print(f"DEBUG: Failed to parse date '{completed_at}': {parse_error}")
                        continue
                else:
                    print(f"DEBUG: completed_at is not a string: {type(completed_at)}")
                    continue
                
                # Check if the item is within our date range
                if start_datetime <= item_datetime < end_datetime:
                    matched_count += 1
                    item_date = item_datetime.strftime("%Y-%m-%d")
                    
                    # Convert ml_cost to float
                    if isinstance(ml_cost, Decimal):
                        cost_amount = float(ml_cost)
                    elif isinstance(ml_cost, (int, float)):
                        cost_amount = float(ml_cost)
                    elif isinstance(ml_cost, str):
                        try:
                            cost_amount = float(ml_cost)
                        except ValueError:
                            print(f"DEBUG: Could not convert ml_cost to float: {ml_cost}")
                            continue
                    else:
                        print(f"DEBUG: Unexpected ml_cost type: {type(ml_cost)}")
                        continue
                    
                    # Add to daily costs with Bedrock-like structure
                    daily_perplexity_costs[item_date].append({
                        "usage_type": "API-Call",
                        "region": "us-east-1",  # Default region for API calls
                        "amount": cost_amount
                    })
                    
                    if matched_count <= 5:  # Show first few matches for debugging
                        print(f"DEBUG: Match {matched_count} - Date: {item_date}, Cost: {cost_amount}")
                    
            except Exception as e:
                error_count += 1
                if error_count <= 5:  # Show first few errors for debugging
                    print(f"DEBUG: Error processing item {processed_count}: {e}")
                continue
        
        print(f"DEBUG: Processing complete - Processed: {processed_count}, Matched: {matched_count}, Errors: {error_count}")
        print(f"DEBUG: Daily costs found for dates: {list(daily_perplexity_costs.keys())}")
        
        # Convert to the same format as Bedrock data
        formatted_costs = defaultdict(lambda: defaultdict(list))
        for date, cost_list in daily_perplexity_costs.items():
            formatted_costs[date]["Perplexity API"] = cost_list
            print(f"DEBUG: Date {date} has {len(cost_list)} Perplexity API calls")
        
        print(f"DEBUG: Returning formatted costs with {len(formatted_costs)} dates")
        return formatted_costs
        
    except Exception as e:
        print(f"DEBUG: Exception in get_perplexity_costs: {e}")
        raise Exception(f"Error fetching Perplexity cost data: {e}")


def aggregate_cost_data(claude_daily, bedrock_daily, perplexity_daily):
    """Aggregate daily cost data into model and region summaries"""
    
    print(f"DEBUG: Aggregating data - Claude: {len(claude_daily)} days, Bedrock: {len(bedrock_daily)} days, Perplexity: {len(perplexity_daily)} days")
    
    # Combine all data
    all_data = {}
    for date, services in claude_daily.items():
        if date not in all_data:
            all_data[date] = {}
        all_data[date].update(services)
    
    for date, services in bedrock_daily.items():
        if date not in all_data:
            all_data[date] = {}
        all_data[date].update(services)
    
    for date, services in perplexity_daily.items():
        if date not in all_data:
            all_data[date] = {}
        all_data[date].update(services)
        print(f"DEBUG: Added Perplexity data for date {date}: {services}")
    
    print(f"DEBUG: Combined data has {len(all_data)} dates")
    
    # Initialize aggregation structures
    model_totals = defaultdict(float)
    model_regions = defaultdict(lambda: defaultdict(float))
    region_totals = defaultdict(float)
    region_models = defaultdict(lambda: defaultdict(float))
    grand_total = 0
    
    # Track API call counts for Perplexity
    perplexity_call_count = 0
    
    # Aggregate data
    for date, services in all_data.items():
        for service, usage_list in services.items():
            for usage_data in usage_list:
                amount = usage_data['amount']
                region = usage_data['region']
                
                # Count API calls for Perplexity
                if service == "Perplexity API":
                    perplexity_call_count += 1
                    print(f"DEBUG: Found Perplexity API usage - Date: {date}, Amount: {amount}")
                
                # Add to model totals
                model_totals[service] += amount
                model_regions[service][region] += amount
                
                # Add to region totals
                region_totals[region] += amount
                region_models[region][service] += amount
                
                # Add to grand total
                grand_total += amount
    
    print(f"DEBUG: Model totals: {dict(model_totals)}")
    print(f"DEBUG: Perplexity API call count: {perplexity_call_count}")
    
    # Build model summary
    by_model = {}
    for model, total_cost in model_totals.items():
        percentage = round((total_cost / grand_total) * 100, 1) if grand_total > 0 else 0
        regions = {region: {"cost": round(cost, 4)} for region, cost in model_regions[model].items()}
        
        model_data = {
            "total_cost": round(total_cost, 4),
            "percentage": percentage,
            "regions": regions
        }
        
        # Add API call count for Perplexity
        if model == "Perplexity API":
            model_data["api_calls"] = perplexity_call_count
        
        by_model[model] = model_data
    
    # Build region summary
    by_region = {}
    for region, total_cost in region_totals.items():
        models = {model: {"cost": round(cost, 4)} for model, cost in region_models[region].items()}
        
        by_region[region] = {
            "total_cost": round(total_cost, 4),
            "models": models
        }
    
    return {
        "by_model": by_model,
        "by_region": by_region
    }, grand_total


def lambda_handler(event, context):
    try:
        # Check API key
        headers = event.get("headers") or {}
        key = headers.get("x-outlaw-api-key")
        if key != API_KEY:
            return {"statusCode": 401, "body": json.dumps({"message": "Unauthorized"})}

        # Parse POST body
        body = event.get("body")
        if body:
            body = json.loads(body)
        else:
            body = {}

        days = int(body.get("days", 40))  # default 40 days

        end = datetime.today().date()
        start = end - timedelta(days=days)

        print(f"DEBUG: Analysis period - Start: {start}, End: {end}, Days: {days}")

        claude_services = [
            "Claude Sonnet 4 (Amazon Bedrock Edition)",
            "Claude 3.5 Sonnet (Amazon Bedrock Edition)",
            "Claude 3 Sonnet (Amazon Bedrock Edition)",
            "Claude 3 Haiku (Amazon Bedrock Edition)",
            "Claude 3 Opus (Amazon Bedrock Edition)",
            "Claude 2.1 (Amazon Bedrock Edition)",
            "Claude 2 (Amazon Bedrock Edition)",
            "Claude Instant (Amazon Bedrock Edition)",
        ]
        bedrock_services = ["Amazon Bedrock"]

        # Get daily costs
        claude_daily = get_service_costs(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), claude_services)
        bedrock_daily = get_service_costs(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), bedrock_services)
        
        print(f"DEBUG: Got Claude data for {len(claude_daily)} days")
        print(f"DEBUG: Got Bedrock data for {len(bedrock_daily)} days")
        
        # Get Perplexity costs
        perplexity_daily = get_perplexity_costs(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        
        print(f"DEBUG: Got Perplexity data for {len(perplexity_daily)} days")

        # Aggregate the data into the expected format
        model_usage_summary, grand_total = aggregate_cost_data(claude_daily, bedrock_daily, perplexity_daily)

        # Build report
        report = {
            "period": {
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "days_analyzed": days
            },
            "model_usage_summary": model_usage_summary,
            "grand_total": round(grand_total, 4),
            "timestamp": datetime.utcnow().isoformat(),
            "debug_info": {
                "perplexity_days_found": len(perplexity_daily),
                "total_models_found": len(model_usage_summary["by_model"])
            }
        }

        # Save to DynamoDB (store as JSON string to avoid DynamoDB type formatting)
        track_id = str(uuid.uuid4())
        table = dynamodb.Table(DYNAMODB_TABLE)
        table.put_item(Item={
            "track_id": track_id, 
            "report": json.dumps(report),  # Store as JSON string
            "created_at": datetime.utcnow().isoformat()
        })

        return {"statusCode": 200, "body": json.dumps({"message": "Report saved", "track_id": track_id}, indent=2)}

    except Exception as e:
        print(f"DEBUG: Main exception: {e}")
        return {"statusCode": 500, "body": json.dumps({"message": "Error", "error": str(e)})}