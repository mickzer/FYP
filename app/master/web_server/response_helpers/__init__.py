import json
from master.db.models import SQLAlchemyEncoder

def not_implemented(output=None):
	return json.dumps({'error': 'clienterror', 'output': output})

def unauthorized(output='your are not authenticated'):
	return json.dumps({'error': 'clienterror', 'output': output})

def forbidden(output='your are not permitted to perform this action'):
	return json.dumps({'error': 'clienterror', 'output': output})

def bad_request(output='invalid/incomplete parameters', integer=False, int_or_null=False, not_exist=False):
	if integer:
		output += ' must be an Integer'
	elif int_or_null:
		output += ' must be an Integer or Null'
	elif not_exist:
		output += ' does not exist'
	return json.dumps({'error': 'clienterror', 'output': output })

def not_found(output='requested resource could not be found'):
	return json.dumps({'error': 'clienterror', 'output': output })

def internal_error(output=None):
	return json.dumps({'error': 'servererror', 'output': output })

def conflict(output=None):
	return json.dumps({'error': 'clienterror', 'output': 'Value for unique entity '+output+' already exists'})

def json_out(data, exclude=[]):
	return json.dumps([elem.to_dict(exclude=exclude) for elem in data], cls=SQLAlchemyEncoder, indent=2, separators=(',', ': ')) if isinstance(data, list) else data.to_json(exclude=exclude)
