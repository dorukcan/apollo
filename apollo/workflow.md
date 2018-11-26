Pseudo-code of Apollo workflow
---

tasks.py ->
    
    from .backend import Task
    task_product()
        Task(module_path, site_type, crawlers)

-----------------------------------------------
-----------------------------------------------

__main__.py -> start

    from .backend import Engine
    Engine().run()

-----------------------------------------------

backend/engine.py -> Engine

    task_list = self.detect_tasks()
        MODULE LOADER
    for task in task_list:
        task.run(self.db)

backend/engine.py -> Task

    for site_name in self.crawlers:
        self.do_crawl(db, site_name)
            MODULE LOADER

-----------------------------------------------
-----------------------------------------------

product.py -> Market
backend/site.py -> Site

    def collect(self):
        self.print_metadata()
    
        crawler = self.load_module() 
            module = importlib.import_module(self.module_path)
            crawler = module.Crawler()
           
        products = self.collect_site(crawler)
            products = crawler.collect()
            
        old_products = self.get_old_items()
            raise NotImplementedError
    
        payload = self.extract_new_items(products, old_products)
            for new_product in new_products:
                not_valid = False
                conditions = []
                
                for old_product in old_products:
                    if conditions(old_product) == conditions(new_product):
                        not_valid = True
                    break
    
                if not not_valid:
                    payload.append(new_product)
    
        self.save_data(payload)
            self.db.insert_data(self.table_name, payload)
