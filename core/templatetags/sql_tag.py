from django import template

register = template.Library()


@register.filter(name="parameter_join")
def parameter_join(value):
    return ',\n'.join(
        ["'{}'='{}'".format(k, v) for k, v in value.items()]
    )
