import boto3
import json
import os
import uuid
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal


ce = boto3.client("ce", region_name="us-east-1")

dynamodb = boto3.resource("dynamodb")

# Env variable
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "bedrock_usage")

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

        # Combine and calculate grand total
        all_dates = set(list(claude_daily.keys()) + list(bedrock_daily.keys()))
        grand_total = 0
        for date in all_dates:
            date_total = 0
            for service, usage_list in claude_daily.get(date, {}).items():
                date_total += sum(u["amount"] for u in usage_list)
            for service, usage_list in bedrock_daily.get(date, {}).items():
                date_total += sum(u["amount"] for u in usage_list)
            grand_total += date_total

        # Build report
        report = {
            "period": {
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "days_analyzed": days
            },
            "grand_total": grand_total,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Convert all floats to Decimal for DynamoDB
        from decimal import Decimal

        def convert_floats_to_decimal(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats_to_decimal(i) for i in obj]
            else:
                return obj

        report_decimal = convert_floats_to_decimal(report)

        # Save to DynamoDB
        track_id = str(uuid.uuid4())
        table = dynamodb.Table(DYNAMODB_TABLE)
        table.put_item(Item={"track_id": track_id, "report": report_decimal})

        return {"statusCode": 200, "body": json.dumps({"message": "Report saved", "track_id": track_id}, indent=2)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"message": "Error", "error": str(e)})}
