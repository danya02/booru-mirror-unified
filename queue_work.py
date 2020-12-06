from database import *
import functools
import traceback

ENTITY_IMPORT_METHODS = dict()

def to_import_entity(imageboard, ent_type):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped(queued_ent):
            queued_ent.row_locked = True
            queued_ent.save()
            try:
                props = func(queued_ent.entity_local_id, additional_data=queued_ent.additional_data)
                queued_ent.report_success(and_mark_as_final=props.get('MARK_FINAL', False))
                return True, ret_val
            except:
                queued_ent.report_error(traceback.print_exc())
                traceback.print_exc()
                return False, None
        ENTITY_IMPORT_METHODS[imageboard] = ENTITY_IMPORT_METHODS.get(imageboard, dict())
        ENTITY_IMPORT_METHODS[imageboard].update({ent_type: wrapped})
        return wrapped
    return wrapper

def import_single_entity():
    with db.atomic():
        task = QueuedImportEntity.tasks_query().get()
        print('Taken task: ' + str(task))
        result, props = ENTITY_IMPORT_METHODS[task.board.name][task.entity_type.name](task)
        print('Succeded' if result else 'Failed')

