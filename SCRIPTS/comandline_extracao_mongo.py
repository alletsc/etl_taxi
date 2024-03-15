import subprocess

class MongoExporter:
    def __init__(self, start_date, end_date, collection, fields, uri):
        self.start_date = start_date
        self.end_date = end_date
        self.collection = collection
        self.fields = fields
        self.uri = uri

    def export_collection(self):
        command = f"""
        mongoexport --collection {self.collection} --type=csv --out=races.csv --query='{{"createdAt":{{"$gte": {{"$date": "{self.start_date}T00:00:00.000-0300"}},"$lt": {{"$date": "{self.end_date}T00:00:00.000-0300"}}}}}}' --fields={self.fields} --uri {self.uri}
        """.strip()

        try:
            subprocess.run(command, check=True, shell=True)
            print("MongoDB collection exported successfully.")
        except subprocess.CalledProcessError as e:
            print("Error during mongoexport:", e)

if __name__ == "__main__":
    start_date = "2024-01-01"
    end_date = "2024-02-22"
    collection = "races"
    fields = """
    _id,
    event,
    estimatedDuration,
    passenger,
    city,
    broadcastQtd,
    isSuspect,
    isInvalid,
    'billing.associatedPaymentMethod',
    'billing.finalPrice.totalToPay',
    'billing.finalPrice.totalPriceToll',
    'billing.finalPrice.totalWithDiscount',
    'billing.finalPrice.totalWithoutDiscount',
    status,
    createdAt,
    'geolocation.effective.origin.position.lat',
    'geolocation.effective.origin.position.lng',
    'geolocation.effective.destination.position.lng',
    'geolocation.effective.destination.position.lat',
    cancelledAt,
    car,
    driver,
    startedAt,
    finishedAt,
    'reasonForInvalid',
    expiredAt,
    'reasonForSuspect',
    'rating.score',
    'billing.associatedDiscount'
    """
    uri = "mongodb://joao_vieira:RoAYINWMmsy723jVivCZ@localhost:27015/taxirio?retryWrites=true&connectTimeoutMS=100&authSource=taxirio&authMechanism=SCRAM-SHA-1"

    exporter = MongoExporter(start_date, end_date, collection, fields, uri)
    exporter.export_collection()
