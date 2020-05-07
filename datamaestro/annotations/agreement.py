from datamaestro.definitions import DataAnnotation, DatasetDefinition, hook

@hook("pre-use")
def useragreement(definition: DatasetDefinition, message):
    settings = definition.context.user_settings
    answer = settings.agreements.get(definition.id, "no")
    if answer == "yes":
        return
    
    answer = definition.context.ask(message + "\nanswer [yes/no] ", {
        "yes": "yes", "y": "yes",
        "no": "no", "n": "no"
    })

    if answer == "yes":
        settings.agreements[definition.id] = "yes"
        settings.save()
    else:
        raise ValueError("Agreement not accepted")

