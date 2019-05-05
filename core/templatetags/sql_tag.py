from django import template

register = template.Library()


@register.filter(name="parameter_join")
def parameter_join(value):
    return ',\n    '.join(
        ["'{}'='{}'".format(k, v) for k, v in value.items()]
    )

@register.filter(name="field_join")
def field_join(values):
    return ',\n    '.join(values)
