# Util module

def removeNoneFromDict(spec): 
    result = dict()
    for key, item in spec.items():
        if item is None:
            result[key] = 'null'
        else: 
            result[key] = item    
    return result


