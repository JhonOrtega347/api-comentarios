import boto3
import uuid
import os
import json 
from datetime import datetime

def lambda_handler(event, context):
    print("Evento recibido:", event)
        
    try:
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})

        tenant_id = body['tenant_id']
        texto = body['texto']
        
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Error al procesar la entrada: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Entrada JSON inválida o campos faltantes.'})
        }

    
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_name = os.environ["BUCKET_NAME"] 

    uuidv1 = str(uuid.uuid1())
    
    comentario_dynamo = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response_dynamo = table.put_item(Item=comentario_dynamo)
    
    
    comentario_s3 = {
        'timestamp_ingesta': datetime.now().isoformat(),
        'fuente': 'API-Comentario',
        'data': body 
    }
    
    fecha_actual = datetime.now().strftime("%Y/%m/%d")
    file_key = f"{fecha_actual}/{uuidv1}.json" 
    
    s3 = boto3.client('s3')
    try:
        response_s3 = s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(comentario_s3), 
            ContentType='application/json'
        )
        print(f"Archivo JSON ingerido exitosamente en s3://{bucket_name}/{file_key}")
        
    except Exception as e:
        print(f"Error CRÍTICO en la Ingesta S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Error interno al guardar en S3, DynamoDB podría haber sido grabado.'})
        }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'message': 'Comentario creado y datos ingeridos a S3.',
            'uuid': uuidv1,
            'bucket_key': file_key
        })
    }
